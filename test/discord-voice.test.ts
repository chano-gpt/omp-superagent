import { expect, test } from "bun:test";
import { ChannelType, Collection, MessageType, type Message } from "discord.js";
import { DiscordVoice } from "../src/discord-voice";
import type { Route } from "../src/contracts";
import type { openLive } from "../src/openai-live";
import type { openVoiceLink } from "../src/discord-voice-link";

test.each(["api", "subscription", "subscription rejected"] as const)("%s: explicit voice start uses only the selected provider and keeps work bound to written confirmation", async mode => {
  const route: Route = { platform: "discord", accountId: "11", authorityId: "22", guildId: "33", channelId: "44", threadId: null };
  let ready = Promise.withResolvers<void>();
  const rejected = Promise.withResolvers<void>();
  const cards: Array<{ readonly id: string; readonly content: string }> = [];
  const client = { user: { id: route.accountId }, rest: {
    post: async (_path: string, { body, signal }: { body: { content: string }; signal: AbortSignal }) => {
      signal.throwIfAborted();
      const id = String(300 + cards.length);
      cards.push({ id, content: body.content });
      return { id, author: { id: route.accountId, bot: true }, content: body.content, channel_id: route.channelId,
        guild_id: route.guildId, timestamp: new Date().toISOString() };
    },
  } };
  const text = {
    id: route.channelId, type: ChannelType.GuildText, client, guildId: route.guildId,
    guild: {
      channels: { fetch: async () => ({ id: "55", guildId: route.guildId, type: ChannelType.GuildVoice }) },
      voiceStates: { cache: new Map([[route.authorityId, { channelId: "55" }]]) },
    },
    send: async ({ content }: { content: string }) => {
      if (content.startsWith("Voice ready.")) ready.resolve();
      if (content.includes("No API fallback")) rejected.resolve();
    },
  };
  let sequence = 100;
  function message(content: string, replyTo?: string): Message {
    return { content, id: String(++sequence), type: replyTo ? MessageType.Reply : MessageType.Default, channel: text, channelId: text.id,
      author: { id: route.authorityId, bot: false }, guildId: text.guildId, client, attachments: new Collection(),
      messageSnapshots: new Collection(), stickers: new Collection(), editedTimestamp: null, webhookId: null, createdTimestamp: Date.now(),
      reference: replyTo ? { messageId: replyTo, channelId: text.id, guildId: route.guildId } : null } as unknown as Message;
  }
  function latestDraft() {
    const draft = cards.at(-1);
    if (!draft) throw new Error("Expected a posted Discord draft");
    return draft;
  }
  async function newCall(command: string) {
    ready = Promise.withResolvers<void>();
    voice.command(message(command));
    await ready.promise;
  }
  let discord!: Parameters<typeof openVoiceLink>[2];
  let live!: Parameters<typeof openLive>[1];
  let signal!: AbortSignal;
  let opened = 0;
  const fromOwner: Buffer[] = [];
  const toOwner: Buffer[] = [];
  const bind = (handlers: Parameters<typeof openLive>[1], active: AbortSignal): Awaited<ReturnType<typeof openLive>> => {
    live = handlers;
    signal = active;
    return { input: pcm => { fromOwner.push(pcm); }, close() {} };
  };
  let apiCalls = 0;
  const billing = mode === "api" ? { apiKeyEnv: "UNUSED", model: "speech", backendModel: "delegate" }
    : { auth: "subscription" as const };
  const voice = new DiscordVoice({ ...billing, voice: "selected", channelId: "55", textChannelId: text.id,
    maxSessionSeconds: 60 }, route.authorityId, mode === "subscription" ? undefined : "unused-api-key",
  error => { throw error; },
  async (_channel, _owner, handlers) => {
    discord = handlers;
    return { play: pcm => { toOwner.push(pcm); }, clear() {}, close() {} };
  },
  async (_options, handlers, active) => {
    ++apiCalls;
    ++opened;
    return bind(handlers, active);
  }, async (_options, handlers, active) => {
    ++opened;
    if (mode === "subscription rejected") throw new Error("No subscription entitlement");
    return bind(handlers, active);
  });
  try {
    voice.command(message("/voice start"));
    voice.command(message("/voice start"));
    if (mode === "subscription rejected") {
      await rejected.promise;
      expect(apiCalls).toBe(0);
      expect(opened).toBe(1);
      return;
    }
    await ready.promise;
    expect(opened).toBe(1);
    expect(apiCalls).toBe(mode === "api" ? 1 : 0);
    const pcm = Buffer.from([5, 0]);
    discord.audio(pcm);
    live.audio(pcm);
    expect(fromOwner).toEqual([pcm]);
    expect(toOwner).toEqual([pcm]);
    await expect(live.forward("Private remark")).rejects.toThrow("No written permission");
    expect(cards).toEqual([]);
    const privateCall = live;
    voice.command(message("/voice start work"));
    await expect(privateCall.forward("Private audio received before consent")).rejects.toThrow("No written permission");
    expect(opened).toBe(1);
    voice.command(message("/voice stop"));
    if (mode === "subscription") {
      await newCall("/voice start share");
      await live.forward("Read the change summary");
      expect(voice.command(message("/voice confirm", latestDraft().id))).toContain("Read the change summary");
      await expect(live.forward("Another private remark")).rejects.toThrow("No written permission");
      voice.command(message("/voice stop"));
    }

    await newCall("/voice start work");
    await expect(privateCall.forward("Late result from the private call")).rejects.toThrow();
    const workCall = live;
    expect(await live.forward("Inspect the failed check")).toContain("Not yet submitted");
    const confirmed = latestDraft();
    expect(confirmed.content).toContain("Inspect the failed check");
    expect(voice.command(message("/voice confirm"))).toBeNull();
    const queuedConfirmation = message("/voice confirm", confirmed.id);
    // The priority controls can arrive while ordinary Discord intake is waiting.
    voice.command(message("/voice start work"));
    voice.command(message("/voice start share"));
    expect(voice.command(queuedConfirmation)).toContain("Inspect the failed check");
    expect(voice.command(queuedConfirmation)).toBeNull();

    await live.forward("Open the current test report");
    const revoked = latestDraft();
    expect(revoked.id).not.toBe(confirmed.id);
    expect(voice.command(message("/voice work off"))).toBeNull();
    expect(signal.aborted).toBe(false);
    expect(voice.command(message("/voice confirm", revoked.id))).toBeNull();
    await expect(live.forward("Do not post this")).rejects.toThrow("No written permission");
    voice.command(message("/voice start work"));
    await expect(workCall.forward("Do not restore a revoked call")).rejects.toThrow("No written permission");
    voice.command(message("/voice stop"));
    expect(signal.aborted).toBe(true);
    discord.audio(pcm);
    live.audio(pcm);
    await expect(workCall.forward("Too late")).rejects.toThrow();
    expect(fromOwner).toHaveLength(1);
    expect(toOwner).toHaveLength(1);

    const opening = newCall("/voice start work");
    voice.command(message("/voice work off"));
    await opening;
    await expect(live.forward("New call revoked during startup")).rejects.toThrow("No written permission");
    expect(voice.command(message("/voice confirm", revoked.id))).toBeNull();
  } finally {
    voice.close();
  }
});
