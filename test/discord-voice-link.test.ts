import { expect, spyOn, test } from "bun:test";
import { EventEmitter, once } from "node:events";
import {
  DAVESession, getVoiceConnection, NetworkingStatusCode, VoiceConnectionStatus,
  type DiscordGatewayAdapterCreator, type Networking, type NetworkingReadyState,
  type VoiceConnection, type VoiceUDPSocket, type VoiceWebSocket,
} from "@discordjs/voice";
import { Client, Collection, Events, GatewayIntentBits, type Guild, type VoiceChannel, type VoiceState } from "discord.js";
import OpusScript from "opusscript";
import { openVoiceLink } from "./oracles/bun-gateway/discord-voice-link.ts";

const owner = "100000000000000001";
const bot = "100000000000000002";
const other = "100000000000000003";
const channelId = "100000000000000004";
const otherChannel = "100000000000000005";
let nextGuildId = 100000000000000100n;
const silence = Buffer.from([0xf8, 0xff, 0xfe]);

// Use the real SDK voice connection, the real Opus codec and a fake wire/MLS
// state powered exclusively by test events. No Discord login, UDP or WebSocket.
function fixture() {
  const client = new Client({ intents: [GatewayIntentBits.GuildVoiceStates] });
  Object.assign(client, { user: { id: bot } });
  spyOn(client, "isReady").mockReturnValue(true);
  const joined = Promise.withResolvers<VoiceConnection>();
  const guildId = String(nextGuildId++);
  const cache = new Collection<string, VoiceState>();
  const adapter: DiscordGatewayAdapterCreator = methods => {
    client.voice.adapters.set(guildId, methods);
    return {
      sendPayload: payload => {
        if (payload.d.channel_id !== null) {
          const connection = getVoiceConnection(guildId);
          if (!connection) throw new Error("Discord voice did not track its connection before joining");
          joined.resolve(connection);
        }
        return true;
      },
      destroy: () => { client.voice.adapters.delete(guildId); },
    };
  };
  const guild = { id: guildId, available: true, shardId: 0, voiceStates: { cache }, voiceAdapterCreator: adapter } as Guild;
  const channel = { id: channelId, guild, client, joinable: true, speakable: true } as VoiceChannel;
  const events = new EventEmitter();
  const calls: string[] = [];
  const handlers = {
    audio: (pcm: Buffer) => { calls.push("audio"); events.emit("audio", pcm); },
    ended: () => { calls.push("ended"); },
  };
  function place(id: string, channel: string | null, emit = true): void {
    const previous = cache.get(id) ?? { id, channelId: null, guild } as VoiceState;
    const current = { id, channelId: channel, guild } as VoiceState;
    cache.set(id, current);
    if (emit) client.emit(Events.VoiceStateUpdate, previous, current);
  }
  place(owner, channelId, false);

  const ws = new EventEmitter() as VoiceWebSocket;
  const udp = new EventEmitter() as VoiceUDPSocket;
  const peers = new Set([owner]);
  const group = [bot, owner];
  const dave = new DAVESession(1, bot, channelId, {});
  const mls = { ready: false, getUserIds: () => group };
  Object.assign(dave, { session: mls });
  const outbound: Buffer[] = [];
  let prepared: Buffer | undefined;
  const readyState: NetworkingReadyState = {
    code: NetworkingStatusCode.Ready, dave, ws, udp,
    connectionOptions: { channelId, endpoint: "offline.invalid", serverId: guildId, sessionId: "offline", token: "offline", userId: bot },
    connectionData: { connectedClients: peers, encryptionMode: "", nonce: 0, nonceBuffer: Buffer.alloc(24),
      packetsPlayed: 0, secretKey: new Uint8Array(32), sequence: 0, speaking: false, ssrc: 1, timestamp: 0 },
  };
  const wire: Pick<Networking, "state" | "destroy" | "prepareAudioPacket" | "dispatchAudio" | "setSpeaking"> = {
    state: readyState,
    destroy: () => { prepared = undefined; },
    prepareAudioPacket: (packet: Buffer) => { prepared = packet; return packet; },
    dispatchAudio: () => {
      if (!prepared) return false;
      outbound.push(prepared);
      if (!prepared.equals(silence)) events.emit("opus", prepared);
      prepared = undefined;
      return true;
    },
    setSpeaking: () => undefined,
  };
  const networking = Object.assign(new EventEmitter(), wire) as Networking;
  const controller = new AbortController();
  const open = () => openVoiceLink(channel, owner, handlers, controller.signal);
  async function transportReady() {
    const connection = await joined.promise;
    if (connection.state.status !== VoiceConnectionStatus.Signalling) throw new Error("Expected a pending voice join");
    place(bot, channelId);
    connection.state = { status: VoiceConnectionStatus.Ready, adapter: connection.state.adapter, networking };
    return connection;
  }
  async function start() {
    const opening = open();
    mls.ready = true;
    dave.lastTransitionId = 0;
    const connection = await transportReady();
    const link = await opening;
    return { link, connection };
  }
  return { start, open, transportReady, controller, guildId, events, calls, place, ws, peers, group, dave, mls, outbound };
}

function tone(samples: number, channels: 1 | 2, frequency: number, rate: number): Buffer {
  const pcm = Buffer.alloc(samples * channels * 2);
  for (let i = 0; i < samples; i++) {
    const value = Math.round(12_000 * Math.sin(2 * Math.PI * i * frequency / rate));
    pcm.writeInt16LE(value, i * channels * 2);
    if (channels === 2) pcm.writeInt16LE(Math.round(value / 2), i * 4 + 2);
  }
  return pcm;
}

function encodeOwner(): Buffer {
  const codec = new OpusScript(48_000, 2, OpusScript.Application.VOIP);
  try { return codec.encode(tone(960, 2, 480, 48_000), 960); }
  finally { codec.delete(); }
}

async function nextBuffer(events: EventEmitter, event: "audio" | "opus"): Promise<Buffer> {
  const [buffer] = await once(events, event, { signal: AbortSignal.timeout(1_000) });
  if (!Buffer.isBuffer(buffer)) throw new Error(`Missing ${event} frame`);
  return buffer;
}

test("refuses a voice channel with an occupant absent from the guild member cache", async () => {
  const f = fixture();
  f.place(other, channelId, false);
  await expect(f.open()).rejects.toThrow("requires the owner alone");
  expect(getVoiceConnection(f.guildId)).toBeUndefined();
});

test("aborts during DAVE setup without subscribing or calling the active-session end callback", async () => {
  const f = fixture();
  const opening = f.open();
  const connection = await f.transportReady();
  expect(connection.receiver.subscriptions.size).toBe(0);
  const reason = new Error("cancelled by owner");
  f.controller.abort(reason);
  await expect(opening).rejects.toBe(reason);
  expect(getVoiceConnection(f.guildId)).toBeUndefined();
  expect(f.calls).toEqual([]);
});

test("waits for the encrypted pair and downmixes only the owner's real Discord Opus to signed 24 kHz mono", async () => {
  const f = fixture();
  const opening = f.open();
  const connection = await f.transportReady();
  expect(connection.receiver.subscriptions.size).toBe(0);
  f.mls.ready = true;
  f.ws.emit("packet", { op: 6 });
  await Promise.resolve();
  expect(connection.receiver.subscriptions.size).toBe(0);
  f.dave.lastTransitionId = 0;
  connection.emit("transitioned", 0);
  const link = await opening;
  const reference = new OpusScript(24_000, 1, OpusScript.Application.VOIP);
  try {
    expect([...connection.receiver.subscriptions.keys()]).toEqual([owner]);
    const opus = encodeOwner();
    const expected = reference.decode(opus);
    const pcm = nextBuffer(f.events, "audio");
    connection.receiver.subscriptions.get(other)?.push(opus);
    connection.receiver.subscriptions.get(owner)?.push(opus);
    const actual = await pcm;
    expect(actual.length).toBe(960);
    expect(actual).not.toEqual(Buffer.alloc(960));
    expect(actual).toEqual(expected);
    expect(f.calls).toEqual(["audio"]);
  } finally { reference.delete(); link.close(); }
});

test("barge-in drops old queued samples and preserves fresh PCM split inside a sample", async () => {
  const f = fixture();
  const { link, connection } = await f.start();
  const decoder = new OpusScript(48_000, 2, OpusScript.Application.VOIP);
  try {
    const inbound = nextBuffer(f.events, "audio");
    link.play(tone(960, 1, 480, 24_000));
    connection.receiver.subscriptions.get(owner)?.push(encodeOwner());
    await inbound;
    const fresh = Buffer.alloc(640); // A short final tail still has to be sent as a complete Opus frame.
    const outbound = nextBuffer(f.events, "opus");
    link.play(fresh.subarray(0, 1));
    link.play(fresh.subarray(1, 317));
    link.play(fresh.subarray(317));
    expect(decoder.decode(await outbound)).toEqual(Buffer.alloc(3_840));
    expect(f.outbound.length).toBe(1);
  } finally { decoder.delete(); link.close(); }
});

test("renders fragmented nonzero 24 kHz PCM as a 20 ms stereo Opus frame", async () => {
  const f = fixture();
  const { link } = await f.start();
  const decoder = new OpusScript(48_000, 2, OpusScript.Application.VOIP);
  try {
    const source = tone(480, 1, 480, 24_000);
    const packet = nextBuffer(f.events, "opus");
    for (let i = 0; i < source.length; i += 77) link.play(source.subarray(i, i + 77));
    const actual = decoder.decode(await packet);
    expect(actual.length).toBe(3_840);
    // The last 500 samples contain five periods of 480 Hz at the Discord rate.
    // Phase-independent energy excludes byte-swapped, dropped or misclocked audio.
    for (const channelOffset of [0, 2]) {
      let sine = 0, cosine = 0, power = 0;
      for (let i = 460; i < 960; ++i) {
        const sample = actual.readInt16LE(i * 4 + channelOffset);
        sine += sample * Math.sin(2 * Math.PI * i / 100);
        cosine += sample * Math.cos(2 * Math.PI * i / 100);
        power += sample * sample;
      }
      expect(Math.sqrt(power / 500)).toBeGreaterThan(2_000);
      expect(Math.hypot(sine, cosine) / Math.sqrt(power * 250)).toBeGreaterThan(0.85);
    }
  } finally { decoder.delete(); link.close(); }
});

const departures = [
  ["owner moves", (f: ReturnType<typeof fixture>, _connection: VoiceConnection) => f.place(owner, otherChannel)],
  ["bot disconnects", (f, _connection) => f.place(bot, null)],
  ["an unseen member enters", (f, _connection) => f.place(other, channelId)],
  ["the voice gateway observes a newcomer first", (f, _connection) => { f.peers.add(other); f.ws.emit("packet", { op: 6 }); }],
  ["the MLS group gains another member before transition execution", (f, _connection) => { f.group.push(other); f.ws.emit("binary"); }],
  ["the voice SDK starts reconnecting", (_f, connection) => {
    if (connection.state.status === VoiceConnectionStatus.Ready) {
      connection.state = { ...connection.state, status: VoiceConnectionStatus.Connecting };
    }
  }],
  ["the link is aborted", (f, _connection) => f.controller.abort()],
] satisfies readonly (readonly [string, (f: ReturnType<typeof fixture>, connection: VoiceConnection) => void])[];

for (const [scenario, interrupt] of departures) {
  test(`terminates before playback when ${scenario}`, async () => {
    const f = fixture();
    const { link, connection } = await f.start();
    try {
      link.play(tone(480, 1, 480, 24_000));
      interrupt(f, connection);
      link.play(tone(480, 1, 880, 24_000));
      await once(connection, VoiceConnectionStatus.Destroyed, { signal: AbortSignal.timeout(1_000) });
      expect(f.outbound).toEqual([]);
      expect(f.calls).toEqual(["ended"]);
    } finally { link.close(); }
  });
}

test("fails closed when provider playback outruns the bounded Discord queue", async () => {
  const f = fixture();
  const { link, connection } = await f.start();
  try {
    link.play(Buffer.alloc(960_001));
    await once(connection, VoiceConnectionStatus.Destroyed, { signal: AbortSignal.timeout(1_000) });
    expect(f.outbound).toEqual([]);
    expect(f.calls).toEqual(["ended"]);
  } finally { link.close(); }
});
