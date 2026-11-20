import { spawn } from "node:child_process";
import * as fs from "node:fs/promises";
import * as net from "node:net";
import type { Frame, Response } from "./host-protocol";
import type { HostOptions } from "./host";
import type { RpcAbortAndRestoreQueueResult } from "@oh-my-pi/pi-coding-agent/modes/rpc/rpc-types";
import { dirname } from "node:path";
import { isDeepStrictEqual } from "node:util";

/** One ordinary pipe owner; reconnecting clients never become native stdin writers. */
const environment = { ...process.env };
const manifest = JSON.parse(await fs.readFile(process.argv[2]!, "utf8")) as {
  options: Omit<HostOptions, "env">; args: string[]; configPath: string; nativeSocket: string;
  processBackend: Awaited<ReturnType<typeof import("./installed-omp").inspectInstalledOmp>>["processBackend"];
};
const { options, args, configPath, nativeSocket } = manifest;
// Select the same native profile before SDK imports initialize paths or stores.
const { extractProfileFlags } = await import("@oh-my-pi/pi-coding-agent/cli/profile-bootstrap");
const { setProfile, resolveProfileEnv } = await import("@oh-my-pi/pi-utils/dirs");
const bootstrap = extractProfileFlags([...options.command.slice(1), ...args]);
setProfile(bootstrap.profile ?? resolveProfileEnv(environment.OMP_PROFILE, environment.PI_PROFILE));
const { resolveCliArgv } = await import("@oh-my-pi/pi-coding-agent/cli-commands");
const { parseArgs } = await import("@oh-my-pi/pi-coding-agent/cli/args");
const resolvedArgv = resolveCliArgv(bootstrap.argv);
if ("error" in resolvedArgv) throw new Error(resolvedArgv.error);
const nativeArgs = parseArgs(resolvedArgv.argv.slice(1));
const { HostChannel, HostRequestTimeout } = await import("./host-protocol");
type HostChannel = import("./host-protocol").HostChannel;
const { SessionManager } = await import("@oh-my-pi/pi-coding-agent/session/session-manager");
const { MAX_RPC_FRAME_BYTES, MAX_RPC_REASSEMBLED_BYTES } = await import("@oh-my-pi/pi-coding-agent/modes/rpc/rpc-frame");
const { parseSessionEntries } = await import("@oh-my-pi/pi-coding-agent/session/session-loader");
if (!await fs.stat(options.sessionFile).then(() => true, error => {
  if (error.code === "ENOENT") return false;
  throw error;
})) {
  // The owner runs in the exact native environment/profile, including its
  // breadcrumb store. Never manufacture a journal or hand --resume a missing path.
  const session = SessionManager.create(options.cwd, dirname(options.sessionFile));
  try {
    await session.setSessionFile(options.sessionFile);
    await session.ensureOnDisk();
    session.flushSync();
  } finally { await session.close(); }
}
const { publishNativeSession } = await import("./native-sessions");
await publishNativeSession(options.sessionFile, options.cwd);
const child = spawn(options.command[0]!, [...options.command.slice(1), ...args], {
  cwd: options.cwd, env: { ...environment, OMP_SUPERAGENT_HOST_SOCKET: options.socketPath,
    OMP_SUPERAGENT_NATIVE_SOCKET: nativeSocket, OMP_SUPERAGENT_HOST_CONFIG: configPath },
  stdio: ["pipe", "pipe", "inherit"],
});
if (!child.stdin || !child.stdout) throw new Error("Native stdio pipes are unavailable");
const native = new HostChannel(child.stdout, child.stdin);
const peers = new Set<HostChannel>();
const ui = new Map<string, Frame>();
const requests = new Map<string, string>();
const responseRuns = new Map<string, number>();
const pendingGoalResumes = new Map<string, { id: string; generation: number }>();
let fencing = false;
let activeCommands = 0;
let serialCommands: Promise<void> = Promise.resolve();
let interruptCommands: Promise<void> = Promise.resolve();
let generation = 0;
let observations: Promise<void> = Promise.resolve();
let control: HostChannel;
let observationFailure: unknown;
let runOrdinal = 0;
const expectedObservations = new Map<string, Frame>();
const failedObservation = (error: unknown) => { observationFailure = error; fencing = true; };
const requireSuccess = (response: Response) => {
  if (!response.success) throw new Error(response.error ?? "Native request failed");
  return response.data as Frame;
};
const publish = (frame: Frame) => {
  for (const peer of peers) {
    try { peer.send(frame); } catch { peer.destroy(); }
  }
};
const record = async (frame: Frame, observedRun = runOrdinal) => {
  const result = requireSuccess(await control.request({ type: "observe", customType: "rpc_prompt_result", frame,
    runOrdinal: observedRun, hostPid: child.pid }));
  if (typeof result.entryId !== "string") throw new Error("Native durable observation identity was not confirmed");
  expectedObservations.set(result.entryId, frame);
};
native.on("frame", (incoming: Frame) => {
  if (incoming.type === "response") {
    if (typeof incoming.id === "string" && requests.has(incoming.id)) {
      responseRuns.set(incoming.id, runOrdinal);
      if (!["prompt", "abort_and_prompt"].includes(String(incoming.command))) requests.delete(incoming.id);
    }
    return;
  }
  if (incoming.type === "ready") return;
  const frame = { ...incoming };
  if (frame.type === "extension_error" && String(frame.extensionPath).endsWith("/host-extension.ts"))
    failedObservation(new Error("The native lifecycle extension reported an observation failure"));
  if (frame.type === "agent_start") runOrdinal++;
  if (typeof frame.id === "string" && requests.has(frame.id)) frame.id = requests.get(frame.id);
  if (frame.type === "extension_ui_request" && typeof frame.id === "string") {
    if (frame.method === "cancel") ui.delete(String(frame.targetId));
    else if (["select", "confirm", "input", "editor", "ask"].includes(String(frame.method))) ui.set(frame.id, frame);
  }
  if (frame.type === "prompt_result") {
    const resultRun = runOrdinal;
    observations = observations.then(async () => {
      if (observationFailure) return;
      await record(frame, resultRun);
      publish(frame);
    })
      .catch(failedObservation);
    if (typeof incoming.id === "string") requests.delete(incoming.id);
  } else observations = observations.then(() => {
    if (!observationFailure) publish(frame);
  }).catch(failedObservation);
});
await native.ready.promise;
requireSuccess(await native.request({ type: "negotiate_protocol", protocolVersion: 2 }));
// Stock commands queue behind session_start; completion proves our native
// extension finished materialization and opened its private control endpoint.
requireSuccess(await native.request({ type: "get_state" }));
requireSuccess(await native.request({ type: "set_ask_dialog", enabled: true }));
const socket = net.createConnection(nativeSocket);
control = new HostChannel(socket, socket, true);
control.encoder.setProtocolVersion(2);
// The standalone executable cannot load the npm daemon client's native addon in an
// extension. Serve its read-only inventory over the already-connected private channel.
const listServiceUnits = manifest.processBackend === "broker"
  ? (await import("./host-broker-services")).brokerServiceInventory(options.cwd)
  : (await import("./host-services")).listServiceUnits;
control.on("frame", (frame: Frame) => {
  if (frame.type !== "list_service_units" || typeof frame.id !== "string") return;
  const response = { type: "response", command: frame.type, id: frame.id };
  void listServiceUnits(typeof frame.projectDir === "string" ? frame.projectDir : undefined,
    typeof frame.serviceName === "string" ? frame.serviceName : undefined)
    .then(units => control.send({ ...response, success: true, data: { units } }),
      () => control.send({ ...response, success: false, error: "Native service ownership is unavailable" }))
    .catch(() => control.destroy());
});
await control.ready.promise;
const identity = requireSuccess(await control.request({ type: "snapshot" }));
if (identity.hostPid !== child.pid || identity.sessionFile !== options.sessionFile) throw new Error("Native launch identity mismatch");
const initialPreset = options.modelPreset?.preset ?? options.modelSettings;
if (initialPreset) requireSuccess(await control.request({ type: "set_model_preset", name: options.modelPreset?.name,
  preset: { ...initialPreset, defaultThinkingLevel: initialPreset.defaultThinkingLevel
    ?? (identity.modelSettings as Frame).defaultThinkingLevel }, preserveActiveModel: !options.modelPreset }));
const state = async () => {
  await observations;
  if (observationFailure) throw new Error("A durable native observation failed; residency remains held");
  const nativeState = requireSuccess(await (activeCommands
    ? control.request({ type: "get_state" })
    : native.request({ type: "get_state" })));
  const owned = requireSuccess(await control.request({ type: "snapshot" }));
  if (owned.sessionId !== nativeState.sessionId || owned.sessionFile !== nativeState.sessionFile
    || owned.hostPid !== child.pid || owned.generation !== identity.generation || owned.instanceKey !== identity.instanceKey)
    throw new Error("Native identity/generation changed");
  if (typeof owned.hasOwnedRunningWork !== "boolean" || owned.observationHeld === true)
    throw new Error("Native owned work or durable observations cannot be authoritatively established; residency remains held");
  return { ...nativeState, ...owned, adapterOwnerPid: process.pid, adapterProtocol: "owned-stock-stdio-v1",
    pendingUiRequests: [...ui.values()] };
};
const idle = (value: Frame) => value.isSettled === true && value.isStreaming === false
  && value.isCompacting === false && value.hasPendingAsyncWork === false && value.queuedMessageCount === 0
  && value.hasOwnedRunningWork === false && value.admitted === false && value.postPromptWork === false
  && value.mutationHeld === false && !ui.size && !(value.goal as { enabled?: boolean } | null)?.enabled;
const handle = async (command: Frame, peer: HostChannel) => {
  const originalId = typeof command.id === "string" ? command.id : crypto.randomUUID();
  const respond = (response: Frame) => {
    if (peers.has(peer)) peer.send({ ...response, type: "response", command: command.type, id: originalId });
  };
  if (command.type === "extension_ui_response") {
    if (ui.has(originalId)) { native.send(command); ui.delete(originalId); }
    return;
  }
  if (command.type === "negotiate_protocol") {
    if (command.protocolVersion !== 2) { respond({ success: false, error: "Unsupported adapter protocol" }); return; }
    peer.encoder.setProtocolVersion(2);
    respond({ success: true, data: { protocolVersion: 2 } }); return;
  }
  if (command.type === "get_host_admission") {
    respond({ success: true, data: { fenced: fencing, generation, pendingGoalResumes:
      [...pendingGoalResumes.values()].map(resume => ({ ...resume, current: resume.generation === generation })) } });
    return;
  }
  const stop = command.type === "abort" || command.type === "abort_and_restore_queue"
    || command.type === "goal" && ["pause", "drop"].includes(String(command.op));
  if (fencing && command.type !== "get_state" && command.type !== "shutdown_host" && !stop) {
    respond({ success: false, error: "Host admission is fenced" }); return;
  }
  if (command.type === "abort" || command.type === "abort_and_restore_queue" || command.type === "abort_and_prompt"
    || command.type === "goal" && ["pause", "drop"].includes(String(command.op))) {
    generation++;
  }
  if (command.type === "shutdown_host" || command.type === "set_model_preset") {
    fencing = true;
    let effectStarted = false;
    let held = false;
    try {
      if (activeCommands || requests.size || !idle(await state())) throw new Error("Host is busy (session_busy)");
      const confirmed = await state();
      if (activeCommands || requests.size || !idle(confirmed)) throw new Error("Host is busy (session_busy)");
      effectStarted = true;
      const result = await control.request(command);
      if (!result.success) {
        const snapshot = requireSuccess(await control.request({ type: "snapshot" }));
        held = snapshot.mutationHeld === true;
        respond(result);
        return;
      }
      respond(result);
      if (command.type === "shutdown_host") {
        child.stdin!.end();
        return;
      }
    } catch (error) {
      if (effectStarted) {
        held = true;
        peer.destroy();
      } else respond({ success: false, code: "session_busy", error: error instanceof Error ? error.message : String(error) });
    } finally {
      // A successful shutdown never reopens admission.
      if (!held && (command.type !== "shutdown_host" || !child.stdin!.writableEnded)) fencing = false;
    }
    return;
  }
  // Native get_* commands are observations, not admitted execution. A
  // reconnecting reader must not make an otherwise idle shutdown spuriously busy.
  const readOnly = String(command.type).startsWith("get_");
  if (!readOnly) activeCommands++;
  let resumeToken: string | undefined;
  let forwarded = false;
  const admittedGeneration = generation;
  const interrupt = command.type === "abort" || command.type === "abort_and_restore_queue"
    || command.type === "abort_and_prompt";
  const input = ["prompt", "steer", "follow_up", "abort_and_prompt", "steer_subagent", "dictation_start"]
    .includes(String(command.type));
  const background = command.type === "bash" || command.type === "predict_word";
  const priorityQuery = command.type === "get_state" || command.type === "get_session_stats";
  const predecessor = serialCommands;
  const serialDone = interrupt || !input && !background && !priorityQuery ? Promise.withResolvers<void>() : undefined;
  if (serialDone) serialCommands = predecessor.then(() => serialDone.promise);
  const previousInterrupt = interruptCommands;
  const interruptDone = interrupt ? Promise.withResolvers<void>() : undefined;
  if (interruptDone) interruptCommands = previousInterrupt.then(() => interruptDone.promise);
  if (command.type === "goal" && command.op === "resume") {
    resumeToken = crypto.randomUUID();
    pendingGoalResumes.set(resumeToken, { id: originalId, generation: admittedGeneration });
  }
  try {
    // Keep unadmitted input on our side of a blocking native command. A control
    // interrupt may release native compaction before stdin reads its abort frame.
    if (interrupt) await previousInterrupt;
    if (!interrupt && !background && !priorityQuery) await predecessor;
    if ((input || resumeToken) && !interrupt && (generation !== admittedGeneration || fencing))
      throw new Error(resumeToken ? "Goal resume cancelled before admission" : "Input cancelled before native admission");
    if (command.type === "get_state") { respond({ success: true, data: await state() }); return; }
    if (command.type === "get_session_stats") {
      respond(await control.request(command));
      return;
    }
    if (command.type === "get_entries") {
      await observations;
      if (observationFailure) throw new Error("Durable native observations are unavailable");
    }
    const nativeId = crypto.randomUUID();
    // Only queue restoration needs the full RPC-mode module. Load it before
    // the interrupt withdraws any input, not after that effect has happened.
    const fitRestoredQueue = command.type === "abort_and_restore_queue"
      ? (await import("@oh-my-pi/pi-coding-agent/modes/rpc/rpc-mode")).fitAbortAndRestoreQueueResponse : undefined;
    let restored: RpcAbortAndRestoreQueueResult | undefined;
    if (interrupt) {
      // Establish the native abort barrier and withdraw already-admitted input
      // before either UI cancellation or the serial RPC tail can release compact.
      const started = requireSuccess(await control.request({
        type: "interrupt_maintenance", restoreQueue: command.type === "abort_and_restore_queue",
      }, nativeId));
      if (command.type === "abort_and_restore_queue") restored = started as unknown as RpcAbortAndRestoreQueueResult;
      for (const id of ui.keys()) native.send({ type: "extension_ui_response", id, cancelled: true });
      ui.clear();
      // Let the priority abort finish before the stock abort starts: overlapping
      // abort() calls share a boolean barrier, and either finally can lower it.
      requireSuccess(await control.request({ type: "finish_interrupt", token: nativeId }, undefined, 30_000));
    }
    if (!readOnly) requests.set(nativeId, originalId);
    forwarded = true;
    const completion = native.request({ ...command, id: undefined }, nativeId,
      command.type === "compact" ? 0 : 30_000);
    let result = await completion;
    if (result.success && restored && fitRestoredQueue) {
      // The original RPC still stops its builtin/goal controllers and supplies
      // its receipt. Keep any input it withdrew too, in native queue order.
      const remaining = result.data as RpcAbortAndRestoreQueueResult;
      const flags = {
        ...(restored.imagesDropped || remaining.imagesDropped ? { imagesDropped: true as const } : {}),
        ...(restored.truncated || remaining.truncated ? { truncated: true as const } : {}),
      };
      result = fitRestoredQueue(originalId, {
        ...restored, ...remaining,
        steering: [...restored.steering, ...remaining.steering],
        followUp: [...restored.followUp, ...remaining.followUp],
      }, peer.encoder.maxResponseBytes - (Object.keys(flags).length ? Buffer.byteLength(JSON.stringify(flags)) : 0)) as Response;
      Object.assign(result.data as RpcAbortAndRestoreQueueResult, flags);
    }
    if (command.type === "prompt" || command.type === "abort_and_prompt") {
      if (result.success && (result.data as Frame | undefined)?.agentInvoked === false) {
        await record({ type: "prompt_result", id: originalId, agentInvoked: false, status: "completed" }, responseRuns.get(nativeId));
        requests.delete(nativeId);
      } else if (!result.success) requests.delete(nativeId);
    } else requests.delete(nativeId);
    responseRuns.delete(nativeId);
    respond(result);
  } catch (error) {
    if (error instanceof HostRequestTimeout && !forwarded && resumeToken) {
      respond({ success: false, error: "Goal resume cancelled before admission" });
    } else if (error instanceof HostRequestTimeout && !String(command.type).startsWith("get_")) {
      fencing = true;
      peer.destroy();
    } else respond({ success: false, error: error instanceof Error ? error.message : String(error) });
  } finally {
    if (resumeToken) pendingGoalResumes.delete(resumeToken);
    interruptDone?.resolve();
    serialDone?.resolve();
    if (!readOnly) activeCommands--;
  }
};
const server = net.createServer(socket => {
  const peer = new HostChannel(socket, socket, true);
  peers.add(peer);
  peer.on("frame", (frame: Frame) => { void handle(frame, peer).catch(() => peer.destroy()); });
  void peer.exited.then(() => { peers.delete(peer); });
  peer.send({ type: "ready", protocolVersion: 1, supportedProtocolVersions: [1, 2],
    maxFrameBytes: MAX_RPC_FRAME_BYTES, maxReassembledFrameBytes: MAX_RPC_REASSEMBLED_BYTES });
});
await new Promise<void>((resolve, reject) => {
  server.once("error", reject);
  server.listen(options.socketPath, () => { void fs.chmod(options.socketPath, 0o600).then(resolve, reject); });
});
requireSuccess(await control.request({ type: "activate",
  toolsExplicitlySelected: nativeArgs.tools !== undefined, noTools: nativeArgs.noTools === true }));
child.once("error", error => { console.error(error.message); process.exitCode = 1; native.destroy(); });
child.once("exit", (code, signal) => {
  fencing = true;
  void (async () => {
    await observations;
    // Only native exit and an actual readable journal complete persistence.
    const journalText = await fs.readFile(options.sessionFile, "utf8");
    for (const line of journalText.split("\n").filter(Boolean)) JSON.parse(line);
    const journal = parseSessionEntries(journalText);
    const persisted = journalText.endsWith("\n") && journal.some(entry => entry.type === "session" && entry.id === identity.sessionId);
    const observationsFlushed = !observationFailure && [...expectedObservations].every(([id, frame]) =>
      journal.some(entry => entry.id === id && entry.type === "custom" && entry.customType === "rpc_prompt_result"
        && (entry.data as Frame)?.instanceKey === identity.instanceKey
        && isDeepStrictEqual((entry.data as Frame)?.frame, frame)))
      && Array.from({ length: runOrdinal }, (_, index) => index + 1).every(ordinal =>
        journal.filter(entry => entry.type === "custom" && entry.customType === "rpc_agent_start"
          && (entry.data as Frame)?.hostPid === child.pid && (entry.data as Frame)?.instanceKey === identity.instanceKey
          && (entry.data as Frame)?.runOrdinal === ordinal).length === 1);
    const receipt = `${options.socketPath}.exit.json`;
    const temporary = `${receipt}.${crypto.randomUUID()}`;
    await fs.writeFile(temporary, JSON.stringify({ pid: child.pid, ownerPid: process.pid,
      sessionId: identity.sessionId, sessionFile: options.sessionFile, code, signal, persisted,
      observationsFlushed }), { mode: 0o600, flag: "wx" });
    await fs.rename(temporary, receipt);
    if (!persisted || !observationsFlushed) throw new Error("Native exit persistence is unconfirmed; adapter remains fenced");
    for (const peer of peers) peer.destroy();
    control.destroy();
    server.close();
    process.exitCode = code === 0 && !signal ? 0 : 1;
  })().catch(error => { console.error(error.message); process.exitCode = 1; });
});
