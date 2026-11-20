import type { VoiceConnectionState, VoiceWebSocket } from "@discordjs/voice";
import { Events, type VoiceChannel, type VoiceState } from "discord.js";
import type OpusScript from "opusscript";

const PCM_RATE = 24_000;
const FRAME_MS = 20;
const FRAME_SAMPLES = PCM_RATE * FRAME_MS / 1_000;
const FRAME_BYTES = FRAME_SAMPLES * 2;
const MAX_PLAYBACK_BYTES = PCM_RATE * 2 * 20;
const TAIL_WAIT_MS = 2 * FRAME_MS;
const SILENCE = Buffer.from([0xf8, 0xff, 0xfe]);
const OPUS_RESET_STATE = 4028;

class VoiceLinkError extends Error {
  override readonly name = "VoiceLinkError";
}

type VoiceHandlers = {
  readonly audio: (pcm: Buffer) => void;
  readonly ended: () => void;
};

export async function openVoiceLink(
  channel: VoiceChannel, ownerId: string, handlers: VoiceHandlers, signal: AbortSignal,
): Promise<{ play(pcm: Buffer): void; clear(): void; close(): void }> {
  signal.throwIfAborted();
  const { client, guild } = channel;
  const bot = client.user;
  if (!bot || bot.id === ownerId) throw new VoiceLinkError("Discord voice requires the authenticated bot and a separate owner");
  const botId = bot.id;
  const guildLoss = [Events.GuildUnavailable, Events.GuildDelete] as const;

  // Voice states include occupants whose GuildMember isn't cached. channel.members omits those.
  function privateRoom(botRequired: boolean): boolean {
    if (!client.isReady() || !guild.available || guild.voiceStates.cache.get(ownerId)?.channelId !== channel.id) return false;
    const botChannel = guild.voiceStates.cache.get(botId)?.channelId;
    return (botRequired ? botChannel === channel.id : !botChannel || botChannel === channel.id) &&
      !guild.voiceStates.cache.some(state => state.channelId === channel.id && state.id !== ownerId && state.id !== botId);
  }

  if (!privateRoom(false)) throw new VoiceLinkError("Discord voice requires the owner alone in the target channel");
  const [sdk, { default: Opus }] = await Promise.all([
    import("@discordjs/voice"), import("opusscript"),
  ]).catch((cause: unknown) => { throw new VoiceLinkError("Discord voice needs the optional Voice SDK, DAVE binding, and Opus codec", { cause }); });
  signal.throwIfAborted();
  if (!privateRoom(false)) throw new VoiceLinkError("Discord voice membership changed before joining");
  if (guild.voiceStates.cache.get(botId)?.channelId || sdk.getVoiceConnection(guild.id) || client.voice.adapters.has(guild.id)) {
    throw new VoiceLinkError("The Discord bot is already connected to voice in this server");
  }
  if (!channel.joinable || !channel.speakable) throw new VoiceLinkError("The Discord bot needs permission to join and speak");

  const connection = sdk.joinVoiceChannel({
    channelId: channel.id, guildId: guild.id, adapterCreator: guild.voiceAdapterCreator,
    selfDeaf: false, selfMute: false, daveEncryption: true,
  });
  const setup = AbortSignal.any([signal, AbortSignal.timeout(15_000)]);
  const ready = Promise.withResolvers<void>();
  let phase: "joining" | "open" | "closed" = "joining";
  let closeReason: unknown;
  let ws: VoiceWebSocket | undefined;
  let receiver: ReturnType<typeof connection.receiver.subscribe> | undefined;
  let decoder: OpusScript | undefined;
  let encoder: OpusScript | undefined;
  let speaking = false;
  let heardOwnerOnVoiceGateway = false;
  const queued = Buffer.allocUnsafe(MAX_PLAYBACK_BYTES);
  const stereo = Buffer.allocUnsafe(FRAME_BYTES * 4);
  let readAt = 0;
  let bytes = 0;
  let lastAudioAt = 0;
  let previous: number | undefined;
  let silenceFrames = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;

  function terminate(reason: unknown, notify = true): void {
    if (phase === "closed") return;
    const wasOpen = phase === "open";
    phase = "closed";
    closeReason = reason;
    clearTimeout(timer);
    signal.removeEventListener("abort", onAbort);
    setup.removeEventListener("abort", onAbort);
    client.off(Events.VoiceStateUpdate, onVoiceState);
    for (const event of guildLoss) client.off(event, onGuildLost);
    client.off(Events.ChannelDelete, onChannelLost);
    client.off(Events.ShardReconnecting, onShardLost);
    connection.off("stateChange", onConnectionState);
    connection.off("transitioned", refresh);
    ws?.off("packet", refresh).off("binary", refresh);
    connection.receiver.speaking.off("end", onSpeechEnd);
    receiver?.destroy();
    decoder?.delete();
    encoder?.delete();
    // The SDK may still be inside its state setter (which can send a rejoin after
    // emitting stateChange). Stop media here, then destroy after its stack unwinds.
    queueMicrotask(() => {
      try {
        if (connection.state.status !== sdk.VoiceConnectionStatus.Destroyed) connection.destroy();
      } finally {
        connection.off("error", terminate);
      }
    });
    if (!wasOpen) ready.reject(reason);
    else if (notify) handlers.ended();
  }

  function refresh(): boolean {
    if (phase === "closed") return false;
    const state = connection.state;
    const network = state.status === sdk.VoiceConnectionStatus.Ready ? state.networking.state : undefined;
    const voice = network?.code === sdk.NetworkingStatusCode.Ready ? network : undefined;
    const peers = voice?.connectionData.connectedClients;
    const unexpectedPeer = peers && (
      (heardOwnerOnVoiceGateway && !peers.has(ownerId)) || [...peers].some(id => id !== ownerId && id !== botId)
    );
    if (!privateRoom(phase === "open") || unexpectedPeer) return rejectMedia("Discord voice membership changed");
    heardOwnerOnVoiceGateway ||= peers?.has(ownerId) ?? false;
    if (voice?.ws !== ws) {
      ws?.off("packet", refresh).off("binary", refresh);
      ws = voice?.ws;
      ws?.on("packet", refresh).on("binary", refresh);
    }
    const dave = voice?.dave;
    const session = dave?.session;
    // The SDK's SessionMethods omits the native binding's group membership API.
    const members: unknown = session?.ready && "getUserIds" in session &&
      typeof session.getUserIds === "function" && session.getUserIds();
    if (Array.isArray(members) && members.some(id => id !== botId && id !== ownerId)) {
      return rejectMedia("Discord voice encryption group membership changed");
    }
    const encryptedPair = voice?.connectionOptions.channelId === channel.id &&
      dave && dave.protocolVersion > 0 && dave.lastTransitionId !== undefined && !dave.reinitializing &&
      Array.isArray(members) && members.length === 2 && members.includes(ownerId) && members.includes(botId);
    if (!encryptedPair || !privateRoom(true)) {
      return phase === "open" ? rejectMedia("Discord voice lost the verified owner-only encrypted connection") : false;
    }
    ready.resolve();
    return true;
  }

  function rejectMedia(message: string): false {
    terminate(new VoiceLinkError(message));
    return false;
  }

  function onVoiceState(oldState: VoiceState, newState: VoiceState): void {
    if (newState.guild.id !== guild.id) return;
    if (oldState.channelId !== newState.channelId &&
        (newState.id === ownerId || (newState.id === botId && newState.channelId !== channel.id) ||
        (newState.id !== botId && newState.channelId === channel.id))) {
      terminate(new VoiceLinkError("Discord voice membership changed"));
    } else refresh();
  }
  function onConnectionState(oldState: VoiceConnectionState, state: VoiceConnectionState): void {
    if (state.status === sdk.VoiceConnectionStatus.Destroyed || state.status === sdk.VoiceConnectionStatus.Disconnected ||
        (oldState.status === sdk.VoiceConnectionStatus.Ready && state.status !== sdk.VoiceConnectionStatus.Ready)) {
      terminate(new VoiceLinkError("Discord voice disconnected"));
    } else refresh();
  }
  function onGuildLost(lost: { readonly id: string }): void { if (lost.id === guild.id) terminate(new VoiceLinkError("Discord voice server became unavailable")); }
  function onChannelLost(lost: { readonly id: string }): void { if (lost.id === channel.id) terminate(new VoiceLinkError("Discord voice channel was deleted")); }
  function onShardLost(id: number): void { if (id === guild.shardId) terminate(new VoiceLinkError("Discord voice gateway disconnected")); }
  function onAbort(): void { terminate(signal.aborted ? signal.reason : new VoiceLinkError("Timed out waiting for owner-only encrypted Discord voice")); }
  function onSpeechEnd(id: string): void { if (id === ownerId) speaking = false; }

  function schedule(now = performance.now()): void {
    if (phase === "open" && timer === undefined && (bytes >= 2 || silenceFrames > 0)) {
      timer = setTimeout(send, Math.max(0, FRAME_MS - (performance.now() - now)));
    }
  }

  function send(): void {
    const now = performance.now();
    timer = undefined;
    if (!encoder || !refresh()) return;
    try {
      const sampleBytes = Math.min(FRAME_BYTES, bytes - (bytes % 2));
      if (sampleBytes === FRAME_BYTES || (sampleBytes > 0 && now - lastAudioAt >= TAIL_WAIT_MS)) {
        for (let offset = 0; offset < FRAME_BYTES; offset += 2) {
          const index = (readAt + offset) % queued.length;
          const sample = offset < sampleBytes ? queued.readInt16LE(index) : 0;
          const interpolated = Math.round(((previous ?? sample) + sample) / 2);
          stereo.writeInt16LE(interpolated, offset * 4);
          stereo.writeInt16LE(interpolated, offset * 4 + 2);
          stereo.writeInt16LE(sample, offset * 4 + 4);
          stereo.writeInt16LE(sample, offset * 4 + 6);
          previous = sample;
        }
        readAt = (readAt + sampleBytes) % queued.length;
        bytes -= sampleBytes;
        connection.playOpusPacket(encoder.encode(stereo, FRAME_SAMPLES * 2));
        silenceFrames = 5;
      } else if (silenceFrames > 0) {
        connection.playOpusPacket(SILENCE);
        if (--silenceFrames === 0) connection.setSpeaking(false);
      }
      schedule(now);
    } catch (error) {
      terminate(error);
    }
  }

  function clear(): void {
    if (phase !== "open") return;
    readAt = 0;
    bytes = 0;
    previous = undefined;
    try { encoder?.encoderCTL(OPUS_RESET_STATE, 0); }
    catch (error) { terminate(error); }
  }

  function play(pcm: Buffer): void {
    if (!pcm.length || phase !== "open" || !refresh()) return;
    if (pcm.length > queued.length - bytes) {
      terminate(new VoiceLinkError("Discord voice playback exceeded its 20-second buffer"));
      return;
    }
    const at = (readAt + bytes) % queued.length;
    const first = Math.min(queued.length - at, pcm.length);
    pcm.copy(queued, at, 0, first);
    if (first < pcm.length) pcm.copy(queued, 0, first);
    bytes += pcm.length;
    lastAudioAt = performance.now();
    schedule(lastAudioAt);
  }

  signal.addEventListener("abort", onAbort, { once: true });
  setup.addEventListener("abort", onAbort, { once: true });
  client.on(Events.VoiceStateUpdate, onVoiceState);
  for (const event of guildLoss) client.on(event, onGuildLost);
  client.on(Events.ChannelDelete, onChannelLost);
  client.on(Events.ShardReconnecting, onShardLost);
  connection.on("error", terminate);
  connection.on("transitioned", refresh);
  connection.on("stateChange", onConnectionState);
  try {
    if (setup.aborted) onAbort();
    else onConnectionState(connection.state, connection.state);
    await ready.promise;
    signal.throwIfAborted();
    if (!refresh()) throw closeReason ?? new VoiceLinkError("Discord voice stopped before startup completed");
    // Opus itself resamples and downmixes Discord's stereo into 24 kHz mono.
    decoder = new Opus(PCM_RATE, 1, Opus.Application.VOIP);
    encoder = new Opus(48_000, 2, Opus.Application.VOIP);
    receiver = connection.receiver.subscribe(ownerId, { end: { behavior: sdk.EndBehaviorType.Manual } });
    receiver.on("error", terminate);
    receiver.on("close", () => terminate(new VoiceLinkError("Discord voice receive stream closed")));
    connection.receiver.speaking.on("end", onSpeechEnd);
    receiver.on("data", (packet: Buffer) => {
      if (!decoder || !refresh()) return;
      if (packet.equals(SILENCE)) { speaking = false; return; }
      try {
        const pcm = decoder.decode(packet);
        if (!speaking) {
          speaking = true;
          clear();
        }
        if (phase === "open") handlers.audio(pcm);
      } catch (error) {
        terminate(error);
      }
    });
    phase = "open";
    setup.removeEventListener("abort", onAbort);
    return { play, clear, close: () => terminate(new VoiceLinkError("Discord voice closed"), false) };
  } catch (error) {
    terminate(error, false);
    throw error;
  }
}
