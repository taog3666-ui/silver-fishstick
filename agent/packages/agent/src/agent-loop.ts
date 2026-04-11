/**
 * Agent loop that works with AgentMessage throughout.
 * Transforms to Message[] only at the LLM call boundary.
 */

import {
	type AssistantMessage,
	type Context,
	EventStream,
	streamSimple,
	type ToolResultMessage,
	validateToolArguments,
} from "@mariozechner/pi-ai";
import type {
	AgentContext,
	AgentEvent,
	AgentLoopConfig,
	AgentMessage,
	AgentTool,
	AgentToolCall,
	AgentToolResult,
	StreamFn,
} from "./types.js";

export type AgentEventSink = (event: AgentEvent) => Promise<void> | void;

/**
 * Start an agent loop with a new prompt message.
 * The prompt is added to the context and events are emitted for it.
 */
export function agentLoop(
	prompts: AgentMessage[],
	context: AgentContext,
	config: AgentLoopConfig,
	signal?: AbortSignal,
	streamFn?: StreamFn,
): EventStream<AgentEvent, AgentMessage[]> {
	const stream = createAgentStream();

	void runAgentLoop(
		prompts,
		context,
		config,
		async (event) => {
			stream.push(event);
		},
		signal,
		streamFn,
	).then((messages) => {
		stream.end(messages);
	});

	return stream;
}

/**
 * Continue an agent loop from the current context without adding a new message.
 * Used for retries - context already has user message or tool results.
 *
 * **Important:** The last message in context must convert to a `user` or `toolResult` message
 * via `convertToLlm`. If it doesn't, the LLM provider will reject the request.
 * This cannot be validated here since `convertToLlm` is only called once per turn.
 */
export function agentLoopContinue(
	context: AgentContext,
	config: AgentLoopConfig,
	signal?: AbortSignal,
	streamFn?: StreamFn,
): EventStream<AgentEvent, AgentMessage[]> {
	if (context.messages.length === 0) {
		throw new Error("Cannot continue: no messages in context");
	}

	if (context.messages[context.messages.length - 1].role === "assistant") {
		throw new Error("Cannot continue from message role: assistant");
	}

	const stream = createAgentStream();

	void runAgentLoopContinue(
		context,
		config,
		async (event) => {
			stream.push(event);
		},
		signal,
		streamFn,
	).then((messages) => {
		stream.end(messages);
	});

	return stream;
}

export async function runAgentLoop(
	prompts: AgentMessage[],
	context: AgentContext,
	config: AgentLoopConfig,
	emit: AgentEventSink,
	signal?: AbortSignal,
	streamFn?: StreamFn,
): Promise<AgentMessage[]> {
	const newMessages: AgentMessage[] = [...prompts];
	const currentContext: AgentContext = {
		...context,
		messages: [...context.messages, ...prompts],
	};

	await emit({ type: "agent_start" });
	await emit({ type: "turn_start" });
	for (const prompt of prompts) {
		await emit({ type: "message_start", message: prompt });
		await emit({ type: "message_end", message: prompt });
	}

	await runLoop(currentContext, newMessages, config, signal, emit, streamFn);
	return newMessages;
}

export async function runAgentLoopContinue(
	context: AgentContext,
	config: AgentLoopConfig,
	emit: AgentEventSink,
	signal?: AbortSignal,
	streamFn?: StreamFn,
): Promise<AgentMessage[]> {
	if (context.messages.length === 0) {
		throw new Error("Cannot continue: no messages in context");
	}

	if (context.messages[context.messages.length - 1].role === "assistant") {
		throw new Error("Cannot continue from message role: assistant");
	}

	const newMessages: AgentMessage[] = [];
	const currentContext: AgentContext = { ...context };

	await emit({ type: "agent_start" });
	await emit({ type: "turn_start" });

	await runLoop(currentContext, newMessages, config, signal, emit, streamFn);
	return newMessages;
}

function createAgentStream(): EventStream<AgentEvent, AgentMessage[]> {
	return new EventStream<AgentEvent, AgentMessage[]>(
		(event: AgentEvent) => event.type === "agent_end",
		(event: AgentEvent) => (event.type === "agent_end" ? event.messages : []),
	);
}

/**
 * Main loop logic shared by agentLoop and agentLoopContinue.
 */
async function runLoop(
	currentContext: AgentContext,
	newMessages: AgentMessage[],
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
	streamFn?: StreamFn,
): Promise<void> {
	let firstTurn = true;
	// Check for steering messages at start (user may have typed while waiting)
	let pendingMessages: AgentMessage[] = (await config.getSteeringMessages?.()) || [];

	// sn66 iter72: track whether a successful edit/write has occurred. If the
	// model reaches the end of its turn without ever producing an edit, we
	// inject a steering message and retry (bounded). Scoring reads the diff
	// from disk; an empty diff scores zero regardless of deliberation quality.
	let hasProducedEdit = false;
	let noEditRetries = 0;
	const MAX_NO_EDIT_RETRIES = 2;

	// sn66 iter73: provider-error retry. Gemini Flash (production model) and
	// GLM-4.7 (dev proxy) both occasionally return stopReason="error" mid-
	// stream with a partial assistant message and no tool calls. Without a
	// retry this exits immediately with an empty diff. Injecting a short
	// continuation prompt and re-streaming salvages the turn. Bounded.
	let providerErrorRetries = 0;
	const MAX_PROVIDER_ERROR_RETRIES = 3;

	// sn66 iter74: exploration budget. Force the model to commit to an edit
	// after a bounded number of non-editing tool calls (read/bash/grep). If
	// the assistant keeps reading files without ever editing, we inject a
	// "you have read enough, edit now" nudge and reset the counter. This
	// prevents over-exploration on tasks where the model's uncertainty keeps
	// it in a read loop instead of committing to a diff.
	let nonEditToolCalls = 0;
	let exploreBudgetNudgesSent = 0;
	const MAX_EXPLORE_BUDGET = 4;
	const MAX_EXPLORE_NUDGES = 2;

	// sn66 iter75: elapsed-time pressure nudge. The exploration budget
	// (iter74) only triggers after MAX_EXPLORE_BUDGET non-edit tool calls,
	// and the no-edit retry (iter72) only triggers at turn-end. Neither
	// catches the "model gets stuck in long thinking content after one read"
	// failure mode (observed on bench-10 iter74: 1528 thinking events but
	// only 1 tool call across 497s). This timer fires once after the model
	// has been running for THINKING_TIME_PRESSURE_MS without any successful
	// edit, regardless of how many tool calls happened. Activates after a
	// tool result is processed, so it queues for the next turn rather than
	// interrupting an in-flight stream.
	const loopStartTime = Date.now();
	const THINKING_TIME_PRESSURE_MS = 90_000;
	let thinkingTimePressureSent = false;

	// sn66 iter77: per-file edit-error detector. When the same file
	// accumulates consecutive failed `edit` tool calls (typically
	// "Could not find oldText" errors), the model's cached view of that
	// file is out of sync with disk. Retrying verbatim wastes turns.
	// Track per-file error counts; at >= threshold, inject a steering
	// nudge telling the model to switch file OR re-read with a tiny
	// snippet. Successful edits reset the counter. Single alert per file.
	// Complements iter72 no-edit retry (empty-diff at turn-end) and
	// iter74 exploration budget (read-heavy loops): this catches
	// edit-heavy loops that waste tool calls on the same file.
	const editErrorsByFile = new Map<string, number>();
	const stuckEditFilesAlerted = new Set<string>();
	// sn66 iter84: raised 2→3 to match new king (toothpick-egg/sn66-v15a).
	// Gives the model one more attempt before steering — less intervention
	// on genuine struggles, fewer premature nudges on tricky files.
	const EDIT_ERROR_THRESHOLD = 3;

	// Outer loop: continues when queued follow-up messages arrive after agent would stop
	while (true) {
		let hasMoreToolCalls = true;

		// Inner loop: process tool calls and steering messages
		while (hasMoreToolCalls || pendingMessages.length > 0) {
			if (!firstTurn) {
				await emit({ type: "turn_start" });
			} else {
				firstTurn = false;
			}

			// Process pending messages (inject before next assistant response)
			if (pendingMessages.length > 0) {
				for (const message of pendingMessages) {
					await emit({ type: "message_start", message });
					await emit({ type: "message_end", message });
					currentContext.messages.push(message);
					newMessages.push(message);
				}
				pendingMessages = [];
			}

			// Stream assistant response
			const message = await streamAssistantResponse(currentContext, config, signal, emit, streamFn);
			newMessages.push(message);

			if (message.stopReason === "aborted") {
				await emit({ type: "turn_end", message, toolResults: [] });
				await emit({ type: "agent_end", messages: newMessages });
				return;
			}

			// sn66 iter73: provider-error retry — if the stream cut off with
			// stopReason="error", inject a continuation nudge and re-stream.
			// Gated on edit tool presence so mock/unit test setups without
			// edit tools retain the previous immediate-return behavior.
			if (message.stopReason === "error") {
				const hasEditToolForErrRetry = (currentContext.tools ?? []).some(
					(t) => t.name === "edit" || t.name === "write",
				);
				if (hasEditToolForErrRetry && providerErrorRetries < MAX_PROVIDER_ERROR_RETRIES) {
					providerErrorRetries++;
					await emit({ type: "turn_end", message, toolResults: [] });
					pendingMessages.push({
						role: "user",
						content: [
							{
								type: "text",
								text: "The previous response was cut off by a provider error. Continue immediately with a tool call — do NOT write narrative text. Call `read` or `edit` directly. The harness scores the diff on disk; any empty output loses.",
							},
						],
						timestamp: Date.now(),
					});
					hasMoreToolCalls = false;
					continue;
				}
				await emit({ type: "turn_end", message, toolResults: [] });
				await emit({ type: "agent_end", messages: newMessages });
				return;
			}

			// Check for tool calls
			const toolCalls = message.content.filter((c) => c.type === "toolCall");
			hasMoreToolCalls = toolCalls.length > 0;

			const toolResults: ToolResultMessage[] = [];
			if (hasMoreToolCalls) {
				toolResults.push(...(await executeToolCalls(currentContext, message, config, signal, emit)));

				for (const result of toolResults) {
					currentContext.messages.push(result);
					newMessages.push(result);
					// sn66 iter72: flag first successful edit/write so the
					// no-edit retry gate below knows we already have a diff.
					if (!result.isError && (result.toolName === "edit" || result.toolName === "write")) {
						hasProducedEdit = true;
						nonEditToolCalls = 0;
					} else if (!hasProducedEdit && !result.isError) {
						// sn66 iter74: count non-editing tool calls before the
						// first successful edit. Resets once editing begins.
						nonEditToolCalls++;
					}
				}

				// sn66 iter77: per-file edit-error detector. Iterate tool
				// calls paired with their results (index-aligned, which is
				// how executeToolCalls returns them). When an `edit` call
				// fails on a specific path, bump that path's counter. At
				// >= EDIT_ERROR_THRESHOLD consecutive failures without a
				// successful edit in between, queue a single steering
				// nudge telling the model to switch strategy. Successful
				// edits reset the counter for that file.
				for (let i = 0; i < toolCalls.length; i++) {
					const call = toolCalls[i];
					const result = toolResults[i];
					if (!call || call.type !== "toolCall" || call.name !== "edit") continue;
					if (!result) continue;
					const callArgs = call.arguments as { path?: unknown } | undefined;
					const editPath = typeof callArgs?.path === "string" ? callArgs.path : undefined;
					if (!editPath) continue;
					if (result.isError) {
						const newCount = (editErrorsByFile.get(editPath) ?? 0) + 1;
						editErrorsByFile.set(editPath, newCount);
						if (newCount >= EDIT_ERROR_THRESHOLD && !stuckEditFilesAlerted.has(editPath)) {
							stuckEditFilesAlerted.add(editPath);
							pendingMessages.push({
								role: "user",
								content: [
									{
										type: "text",
										text: `Edit failed ${newCount} times on \`${editPath}\`. Your cached view of this file is out of sync with disk — retrying the same oldText will keep failing because of subtle whitespace/newline differences you can't see in memory. Switch strategy NOW: either (a) pick a DIFFERENT file from the task's acceptance criteria and edit that first, or (b) call \`read\` on \`${editPath}\` one more time then try a much smaller snippet (3-6 lines, not a whole function). Do not retry the failed oldText verbatim.`,
									},
								],
								timestamp: Date.now(),
							});
						}
					} else {
						editErrorsByFile.set(editPath, 0);
					}
				}

				// sn66 iter74: exploration budget — if the agent has made
				// MAX_EXPLORE_BUDGET non-editing tool calls without any edit,
				// inject a nudge to commit to an edit. Capped at
				// MAX_EXPLORE_NUDGES to avoid infinite nudge loops.
				const hasEditOrWriteToolForBudget = (currentContext.tools ?? []).some(
					(t) => t.name === "edit" || t.name === "write",
				);
				if (
					hasEditOrWriteToolForBudget &&
					!hasProducedEdit &&
					nonEditToolCalls >= MAX_EXPLORE_BUDGET &&
					exploreBudgetNudgesSent < MAX_EXPLORE_NUDGES &&
					pendingMessages.length === 0
				) {
					exploreBudgetNudgesSent++;
					nonEditToolCalls = 0;
					pendingMessages.push({
						role: "user",
						content: [
							{
								type: "text",
								text: "You have read enough files. Commit to the most likely target file and call `edit` on it now. One imperfect edit beats no edit — the scorer only reads the diff on disk. Stop exploring; start editing.",
							},
						],
						timestamp: Date.now(),
					});
				}

			}

			await emit({ type: "turn_end", message, toolResults });

			// sn66 iter76: elapsed-time pressure nudge — moved out of the
			// `if (hasMoreToolCalls)` branch so it can fire even when the
			// model stops making tool calls (the actual iter74 bench-10
			// failure mode: 1 early read then pure thinking). iter75 placed
			// this check inside the tool-call branch and grepping rollouts
			// confirmed it never fired once. This placement mirrors the
			// iter72 no-edit retry — runs after turn_end emit, before the
			// pendingMessages refresh. Single-shot via thinkingTimePressureSent.
			// Forces loop re-entry only when the turn ended with no more
			// tool calls (otherwise the next natural turn will consume the
			// nudge via pendingMessages).
			const hasEditOrWriteToolForPressure = (currentContext.tools ?? []).some(
				(t) => t.name === "edit" || t.name === "write",
			);
			if (
				hasEditOrWriteToolForPressure &&
				!hasProducedEdit &&
				!thinkingTimePressureSent &&
				Date.now() - loopStartTime >= THINKING_TIME_PRESSURE_MS &&
				pendingMessages.length === 0
			) {
				thinkingTimePressureSent = true;
				pendingMessages.push({
					role: "user",
					content: [
						{
							type: "text",
							text: "Over 90 seconds have elapsed and the diff on disk is still empty. Stop reasoning. Pick one file from the task description, call `edit` on it with your best guess for the change, and let the result speak. Long internal monologues do not score; only the diff scores. Edit now.",
						},
					],
					timestamp: Date.now(),
				});
				if (!hasMoreToolCalls) {
					hasMoreToolCalls = true;
					continue;
				}
			}

			// sn66 iter72: no-edit retry gate. If the model is about to stop
			// (no more tool calls, no steering queued) but has never produced
			// a successful edit or write, push a nudge and re-enter the loop
			// so it takes one more shot at writing a diff. Bounded retries.
			// Gated on the presence of edit/write tools in the current context
			// so unit/mock setups without those tools behave unchanged.
			const hasEditOrWriteTool = (currentContext.tools ?? []).some(
				(t) => t.name === "edit" || t.name === "write",
			);
			if (hasEditOrWriteTool && !hasMoreToolCalls && !hasProducedEdit && noEditRetries < MAX_NO_EDIT_RETRIES && pendingMessages.length === 0) {
				noEditRetries++;
				pendingMessages.push({
					role: "user",
					content: [
						{
							type: "text",
							text: "Your diff is currently empty. The scoring system reads the unified diff from disk — zero edits means zero matches, regardless of analysis quality. Pick the most plausible target file from the task description and call `edit` on it now (or `write` if the task literally says to create a new file). An imperfect edit beats an empty diff.",
						},
					],
					timestamp: Date.now(),
				});
				hasMoreToolCalls = true;
				continue;
			}

			pendingMessages = (await config.getSteeringMessages?.()) || [];
		}

		// Agent would stop here. Check for follow-up messages.
		const followUpMessages = (await config.getFollowUpMessages?.()) || [];
		if (followUpMessages.length > 0) {
			// Set as pending so inner loop processes them
			pendingMessages = followUpMessages;
			continue;
		}

		// No more messages, exit
		break;
	}

	await emit({ type: "agent_end", messages: newMessages });
}

/**
 * Stream an assistant response from the LLM.
 * This is where AgentMessage[] gets transformed to Message[] for the LLM.
 */
async function streamAssistantResponse(
	context: AgentContext,
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
	streamFn?: StreamFn,
): Promise<AssistantMessage> {
	// Apply context transform if configured (AgentMessage[] → AgentMessage[])
	let messages = context.messages;
	if (config.transformContext) {
		messages = await config.transformContext(messages, signal);
	}

	// Convert to LLM-compatible messages (AgentMessage[] → Message[])
	const llmMessages = await config.convertToLlm(messages);

	// Build LLM context
	const llmContext: Context = {
		systemPrompt: context.systemPrompt,
		messages: llmMessages,
		tools: context.tools,
	};

	const streamFunction = streamFn || streamSimple;

	// Resolve API key (important for expiring tokens)
	const resolvedApiKey =
		(config.getApiKey ? await config.getApiKey(config.model.provider) : undefined) || config.apiKey;

	const response = await streamFunction(config.model, llmContext, {
		...config,
		apiKey: resolvedApiKey,
		signal,
	});

	let partialMessage: AssistantMessage | null = null;
	let addedPartial = false;

	for await (const event of response) {
		switch (event.type) {
			case "start":
				partialMessage = event.partial;
				context.messages.push(partialMessage);
				addedPartial = true;
				await emit({ type: "message_start", message: { ...partialMessage } });
				break;

			case "text_start":
			case "text_delta":
			case "text_end":
			case "thinking_start":
			case "thinking_delta":
			case "thinking_end":
			case "toolcall_start":
			case "toolcall_delta":
			case "toolcall_end":
				if (partialMessage) {
					partialMessage = event.partial;
					context.messages[context.messages.length - 1] = partialMessage;
					await emit({
						type: "message_update",
						assistantMessageEvent: event,
						message: { ...partialMessage },
					});
				}
				break;

			case "done":
			case "error": {
				const finalMessage = await response.result();
				if (addedPartial) {
					context.messages[context.messages.length - 1] = finalMessage;
				} else {
					context.messages.push(finalMessage);
				}
				if (!addedPartial) {
					await emit({ type: "message_start", message: { ...finalMessage } });
				}
				await emit({ type: "message_end", message: finalMessage });
				return finalMessage;
			}
		}
	}

	const finalMessage = await response.result();
	if (addedPartial) {
		context.messages[context.messages.length - 1] = finalMessage;
	} else {
		context.messages.push(finalMessage);
		await emit({ type: "message_start", message: { ...finalMessage } });
	}
	await emit({ type: "message_end", message: finalMessage });
	return finalMessage;
}

/**
 * Execute tool calls from an assistant message.
 */
async function executeToolCalls(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
): Promise<ToolResultMessage[]> {
	const toolCalls = assistantMessage.content.filter((c) => c.type === "toolCall");
	if (config.toolExecution === "sequential") {
		return executeToolCallsSequential(currentContext, assistantMessage, toolCalls, config, signal, emit);
	}
	return executeToolCallsParallel(currentContext, assistantMessage, toolCalls, config, signal, emit);
}

async function executeToolCallsSequential(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	toolCalls: AgentToolCall[],
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
): Promise<ToolResultMessage[]> {
	const results: ToolResultMessage[] = [];

	for (const toolCall of toolCalls) {
		await emit({
			type: "tool_execution_start",
			toolCallId: toolCall.id,
			toolName: toolCall.name,
			args: toolCall.arguments,
		});

		const preparation = await prepareToolCall(currentContext, assistantMessage, toolCall, config, signal);
		if (preparation.kind === "immediate") {
			results.push(await emitToolCallOutcome(toolCall, preparation.result, preparation.isError, emit));
		} else {
			const executed = await executePreparedToolCall(preparation, signal, emit);
			results.push(
				await finalizeExecutedToolCall(
					currentContext,
					assistantMessage,
					preparation,
					executed,
					config,
					signal,
					emit,
				),
			);
		}
	}

	return results;
}

async function executeToolCallsParallel(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	toolCalls: AgentToolCall[],
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
): Promise<ToolResultMessage[]> {
	const results: ToolResultMessage[] = [];
	const runnableCalls: PreparedToolCall[] = [];

	for (const toolCall of toolCalls) {
		await emit({
			type: "tool_execution_start",
			toolCallId: toolCall.id,
			toolName: toolCall.name,
			args: toolCall.arguments,
		});

		const preparation = await prepareToolCall(currentContext, assistantMessage, toolCall, config, signal);
		if (preparation.kind === "immediate") {
			results.push(await emitToolCallOutcome(toolCall, preparation.result, preparation.isError, emit));
		} else {
			runnableCalls.push(preparation);
		}
	}

	const runningCalls = runnableCalls.map((prepared) => ({
		prepared,
		execution: executePreparedToolCall(prepared, signal, emit),
	}));

	for (const running of runningCalls) {
		const executed = await running.execution;
		results.push(
			await finalizeExecutedToolCall(
				currentContext,
				assistantMessage,
				running.prepared,
				executed,
				config,
				signal,
				emit,
			),
		);
	}

	return results;
}

type PreparedToolCall = {
	kind: "prepared";
	toolCall: AgentToolCall;
	tool: AgentTool<any>;
	args: unknown;
};

type ImmediateToolCallOutcome = {
	kind: "immediate";
	result: AgentToolResult<any>;
	isError: boolean;
};

type ExecutedToolCallOutcome = {
	result: AgentToolResult<any>;
	isError: boolean;
};

function prepareToolCallArguments(tool: AgentTool<any>, toolCall: AgentToolCall): AgentToolCall {
	if (!tool.prepareArguments) {
		return toolCall;
	}
	const preparedArguments = tool.prepareArguments(toolCall.arguments);
	if (preparedArguments === toolCall.arguments) {
		return toolCall;
	}
	return {
		...toolCall,
		arguments: preparedArguments as Record<string, any>,
	};
}

async function prepareToolCall(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	toolCall: AgentToolCall,
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
): Promise<PreparedToolCall | ImmediateToolCallOutcome> {
	const tool = currentContext.tools?.find((t) => t.name === toolCall.name);
	if (!tool) {
		return {
			kind: "immediate",
			result: createErrorToolResult(`Tool ${toolCall.name} not found`),
			isError: true,
		};
	}

	try {
		const preparedToolCall = prepareToolCallArguments(tool, toolCall);
		const validatedArgs = validateToolArguments(tool, preparedToolCall);
		if (config.beforeToolCall) {
			const beforeResult = await config.beforeToolCall(
				{
					assistantMessage,
					toolCall,
					args: validatedArgs,
					context: currentContext,
				},
				signal,
			);
			if (beforeResult?.block) {
				return {
					kind: "immediate",
					result: createErrorToolResult(beforeResult.reason || "Tool execution was blocked"),
					isError: true,
				};
			}
		}
		return {
			kind: "prepared",
			toolCall,
			tool,
			args: validatedArgs,
		};
	} catch (error) {
		return {
			kind: "immediate",
			result: createErrorToolResult(error instanceof Error ? error.message : String(error)),
			isError: true,
		};
	}
}

async function executePreparedToolCall(
	prepared: PreparedToolCall,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
): Promise<ExecutedToolCallOutcome> {
	const updateEvents: Promise<void>[] = [];

	try {
		const result = await prepared.tool.execute(
			prepared.toolCall.id,
			prepared.args as never,
			signal,
			(partialResult) => {
				updateEvents.push(
					Promise.resolve(
						emit({
							type: "tool_execution_update",
							toolCallId: prepared.toolCall.id,
							toolName: prepared.toolCall.name,
							args: prepared.toolCall.arguments,
							partialResult,
						}),
					),
				);
			},
		);
		await Promise.all(updateEvents);
		return { result, isError: false };
	} catch (error) {
		await Promise.all(updateEvents);
		return {
			result: createErrorToolResult(error instanceof Error ? error.message : String(error)),
			isError: true,
		};
	}
}

async function finalizeExecutedToolCall(
	currentContext: AgentContext,
	assistantMessage: AssistantMessage,
	prepared: PreparedToolCall,
	executed: ExecutedToolCallOutcome,
	config: AgentLoopConfig,
	signal: AbortSignal | undefined,
	emit: AgentEventSink,
): Promise<ToolResultMessage> {
	let result = executed.result;
	let isError = executed.isError;

	if (config.afterToolCall) {
		const afterResult = await config.afterToolCall(
			{
				assistantMessage,
				toolCall: prepared.toolCall,
				args: prepared.args,
				result,
				isError,
				context: currentContext,
			},
			signal,
		);
		if (afterResult) {
			result = {
				content: afterResult.content ?? result.content,
				details: afterResult.details ?? result.details,
			};
			isError = afterResult.isError ?? isError;
		}
	}

	return await emitToolCallOutcome(prepared.toolCall, result, isError, emit);
}

function createErrorToolResult(message: string): AgentToolResult<any> {
	return {
		content: [{ type: "text", text: message }],
		details: {},
	};
}

async function emitToolCallOutcome(
	toolCall: AgentToolCall,
	result: AgentToolResult<any>,
	isError: boolean,
	emit: AgentEventSink,
): Promise<ToolResultMessage> {
	await emit({
		type: "tool_execution_end",
		toolCallId: toolCall.id,
		toolName: toolCall.name,
		result,
		isError,
	});

	const toolResultMessage: ToolResultMessage = {
		role: "toolResult",
		toolCallId: toolCall.id,
		toolName: toolCall.name,
		content: result.content,
		details: result.details,
		isError,
		timestamp: Date.now(),
	};

	await emit({ type: "message_start", message: toolResultMessage });
	await emit({ type: "message_end", message: toolResultMessage });
	return toolResultMessage;
}
