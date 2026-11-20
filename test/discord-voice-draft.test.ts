import { expect, spyOn, test } from "bun:test";
import {
  ChannelType, MessageFlags, MessageReferenceType, MessageType, REST, Routes,
  type Message, type MessageReference, type TextChannel,
} from "discord.js";
import { VoiceDrafts } from "./oracles/bun-gateway/discord-voice-draft.ts";

const botId = "200";
const ownerId = "300";
const guildId = "400";
const channelId = "500";
const cardId = "600";
const postedAt = 1_000;
const confirm = "/voice confirm";
const work = "/restart is the term to look up in the source tree";

type ReplyFixture = {
  id: string;
  content: string;
  type: MessageType;
  editedTimestamp: number | null;
  createdTimestamp: number;
  webhookId: string | null;
  author: { id: string; bot: boolean };
  client: { user: { id: string } };
  channelId: string;
  guildId: string | null;
  reference: MessageReference | null;
  attachments: ReadonlySet<string>;
  stickers: ReadonlySet<string>;
  messageSnapshots: ReadonlySet<string>;
};

const replyReference: MessageReference = { messageId: cardId, channelId, guildId, type: MessageReferenceType.Default };

function reply(patch: Partial<ReplyFixture> = {}): Message {
  const fields: ReplyFixture = {
    id: "700", content: confirm, type: MessageType.Reply, editedTimestamp: null, createdTimestamp: postedAt + 1,
    webhookId: null, author: { id: ownerId, bot: false }, client: { user: { id: botId } },
    channelId, guildId, reference: replyReference,
    attachments: new Set(), stickers: new Set(), messageSnapshots: new Set(), ...patch,
  };
  return fields as unknown as Message;
}

function draftsFor(rest: Pick<REST, "post">) {
  const channel = {
    id: channelId, guildId, type: ChannelType.GuildText, client: { user: { id: botId }, rest },
  } as unknown as TextChannel;
  const drafts = new VoiceDrafts();
  const session = new AbortController();
  const propose = (text = work, postDeadline?: number) => drafts.propose(channel, ownerId, text, { signal: session.signal, postDeadline });
  return { drafts, session, propose };
}

function fixture(delivery?: Promise<void>, receiptPatch: Readonly<Record<string, unknown>> = {}) {
  const sent: Array<{ id: string; content: string; signal?: AbortSignal }> = [];
  const context = draftsFor({
    async post(route, options) {
      const body = options?.body;
      if (route !== Routes.channelMessages(channelId) || !body || typeof body !== "object" ||
        !("content" in body) || typeof body.content !== "string") throw new Error("Invalid draft request");
      const id = String(Number(cardId) + sent.length);
      sent.push({ ...body, id, content: body.content, signal: options?.signal });
      await delivery;
      return {
        id, author: { id: botId, bot: true }, channel_id: channelId,
        content: body.content, timestamp: new Date(postedAt).toISOString(), ...receiptPatch,
      };
    },
  });
  return { ...context, sent };
}

test.each([
  ["omitted guild", {}],
  ["matching guild and fractional offset timestamp", { guild_id: guildId, timestamp: "1970-01-01T00:00:01.000000+00:00" }],
] as const)("uses the sent card identity for one-shot written confirmation (%s)", async (_receipt, receiptPatch) => {
  const { drafts, propose } = fixture(undefined, receiptPatch);
  await propose();

  const input = drafts.consume(reply());

  expect(input).toMatch(/voice session.*confirmed by the owner in writing/);
  expect(input).toMatch(/not a slash command, native control, or approval/);
  expect(input?.endsWith(`\n\n${work}`)).toBe(true);
  expect(drafts.consume(reply())).toBeNull();
});

const invalidReplies: readonly (readonly [string, Partial<ReplyFixture>])[] = [
  ["other owner", { author: { id: "301", bot: false } }],
  ["bot author", { author: { id: ownerId, bot: true } }],
  ["webhook author", { webhookId: "800" }],
  ["other bot account", { client: { user: { id: "201" } } }],
  ["other guild", { guildId: "401" }],
  ["other channel", { channelId: "501" }],
  ["DM", { guildId: null }],
  ["non-reply", { type: MessageType.Default }],
  ["missing reference", { reference: null }],
  ["other referenced card", { reference: { ...replyReference, messageId: "601" } }],
  ["other referenced channel", { reference: { ...replyReference, channelId: "501" } }],
  ["other referenced guild", { reference: { ...replyReference, guildId: "401" } }],
  ["forward reference", { reference: { ...replyReference, type: MessageReferenceType.Forward } }],
  ["forwarded snapshot", { messageSnapshots: new Set(["800"]) }],
  ["edited reply", { editedTimestamp: postedAt + 2 }],
  ["reply predating the card", { createdTimestamp: postedAt - 1 }],
  ["confirmation with leading whitespace", { content: ` ${confirm}` }],
  ["confirmation with trailing newline", { content: `${confirm}\n` }],
  ["different case", { content: "/VOICE confirm" }],
  ["additional text", { content: `${confirm} and deploy` }],
  ["attachment", { attachments: new Set(["800"]) }],
  ["sticker", { stickers: new Set(["800"]) }],
];

test.each(invalidReplies)("does not consume a draft on %s", async (_violation, patch) => {
  const { drafts, propose } = fixture();
  await propose();

  expect(drafts.consume(reply(patch))).toBeNull();
  expect(drafts.consume(reply())).not.toBeNull();
});

test("rejects a replay of the old confirmation even after a new card is posted", async () => {
  const { drafts, propose, sent } = fixture();
  await propose();
  drafts.consume(reply());
  await propose("Inspect the build state");

  expect(drafts.consume(reply())).toBeNull();
  expect(sent).toHaveLength(2);
  expect(drafts.consume(reply({ id: "701", reference: { ...replyReference, messageId: sent[1].id } }))).toContain("Inspect the build state");
});

test("allows only one unconfirmed card both during send and after Discord returns", async () => {
  const delivery = Promise.withResolvers<void>();
  const { drafts, sent, propose } = fixture(delivery.promise);
  const first = propose();
  try {
    await expect(propose()).rejects.toThrow("already outstanding");
    expect(drafts.consume(reply())).toBeNull();
  } finally {
    delivery.resolve();
    await first;
  }

  await expect(propose("Try replacing the work")).rejects.toThrow("already outstanding");
  expect(sent).toHaveLength(1);
  expect(drafts.consume(reply())).toContain(work);
});

test("does not retry or infer a card ID after an ambiguous Discord send", async () => {
  const delivery = Promise.withResolvers<void>();
  const { drafts, sent, propose } = fixture(delivery.promise);
  const pending = propose();
  delivery.reject(new Error("request timed out"));

  await expect(pending).rejects.toThrow("delivery is uncertain");
  await expect(propose()).rejects.toThrow("already outstanding");
  expect(sent).toHaveLength(1);
  expect(drafts.consume(reply())).toBeNull();
});

test.each([
  ["another channel", { channel_id: "501" }],
  ["another guild", { guild_id: "401" }],
  ["a null guild", { guild_id: null }],
  ["another bot", { author: { id: "201", bot: true } }],
  ["a non-bot author", { author: { id: botId, bot: false } }],
  ["a missing author", { author: null }],
  ["a non-snowflake ID", { id: "not-a-message-id" }],
  ["an out-of-range snowflake", { id: "18446744073709551616" }],
  ["a non-ISO timestamp", { timestamp: "0" }],
  ["an invalid ISO timestamp", { timestamp: "2025-13-01T00:00:00Z" }],
  ["altered content", { content: "Not the submitted card" }],
] as const)("does not bind a Discord receipt with %s", async (_violation, receiptPatch) => {
  const { drafts, propose, sent } = fixture(undefined, receiptPatch);

  await expect(propose()).rejects.toMatchObject({ certainty: "uncertain" });
  expect(drafts.consume(reply())).toBeNull();
  await expect(propose()).rejects.toThrow("already outstanding");
  expect(sent).toHaveLength(1);
});

test("a card cannot be confirmed at its expiry even though the session remains active", async () => {
  const clock = spyOn(Date, "now").mockReturnValue(500);
  try {
    const { drafts, session, propose, sent } = fixture();
    await propose();
    clock.mockReturnValue(500 + 5 * 60 * 1000);

    expect(drafts.consume(reply())).toBeNull();
    expect(session.signal.aborted).toBe(false);
    await propose();
    expect(sent).toHaveLength(2);
  } finally {
    clock.mockRestore();
  }
});

test("aborting the live session invalidates its card and prevents new sends", async () => {
  const { drafts, sent, session, propose } = fixture();
  await propose();
  session.abort();

  expect(drafts.consume(reply())).toBeNull();
  await expect(propose()).rejects.toThrow();
  expect(sent).toHaveLength(1);
});

test("clearing a session while Discord is sending cannot activate its late card", async () => {
  const delivery = Promise.withResolvers<void>();
  const { drafts, propose } = fixture(delivery.promise);
  const pending = propose();
  drafts.clear();
  delivery.resolve();

  await expect(pending).rejects.toThrow("no longer active");
  expect(drafts.consume(reply())).toBeNull();
});

test.each(["clear", "session abort", "posting deadline"] as const)("%s cancels a draft queued behind an in-flight Discord REST request", async cancel => {
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const transmitted: unknown[] = [];
  const rest = new REST({
    retries: 0, rejectOnRateLimit: () => true,
    async makeRequest(_url, options) {
      transmitted.push(options.body);
      if (transmitted.length === 1) {
        entered.resolve();
        await release.promise;
      }
      return new Response(null, { status: 204 });
    },
  }).setToken("fixture-token");
  const route = Routes.channelMessages(channelId);
  const { drafts, propose, session } = draftsFor(rest);
  const blocker = { content: "Block this route's first request" };
  const probe = { content: "Next request after the canceled draft" };
  const inFlight = rest.post(route, { body: blocker });
  let proposal: Promise<void> | undefined;
  const deadline = Promise.withResolvers<never>();
  const timer = setTimeout(() => deadline.reject(new Error("Queued draft did not cancel before transport release")), 1000);
  try {
    await Promise.race([entered.promise, deadline.promise]);
    proposal = propose(work, cancel === "posting deadline" ? performance.now() + 100 : undefined);
    const outcome = Promise.race([proposal, deadline.promise]).catch((error: unknown) => error);
    // Let the SDK's async request preparation enqueue behind the blocked wire call.
    await new Promise<void>(resolve => setImmediate(resolve));
    if (cancel === "clear") drafts.clear();
    if (cancel === "session abort") session.abort();
    expect(await outcome).toMatchObject({ certainty: "uncertain" });
    expect(drafts.consume(reply())).toBeNull();
    expect(transmitted).toEqual([JSON.stringify(blocker)]);
    release.resolve();
    await inFlight;
    await rest.post(route, { body: probe });
    expect(transmitted).toEqual([JSON.stringify(blocker), JSON.stringify(probe)]);
  } finally {
    clearTimeout(timer);
    release.resolve();
    await Promise.allSettled([inFlight, proposal]);
    rest.clearHashSweeper();
    rest.clearHandlerSweeper();
  }
});

test("a verified card can still be confirmed after its posting deadline", async () => {
  const { drafts, propose, sent } = fixture();
  await propose(work, performance.now() + 100);
  const signal = sent[0]?.signal;
  if (!signal) throw new Error("Discord did not receive a posting deadline signal");
  if (!signal.aborted) await new Promise<void>(resolve => {
    signal.addEventListener("abort", () => resolve(), { once: true });
  });

  expect(drafts.consume(reply())).toContain(work);
});

test("renders proposal content literally and disables Discord mentions and embeds", async () => {
  const { drafts, sent, propose } = fixture();
  const injection = "```\n@everyone <@300> <#500> [fake control](https://example.test)\n\u202E\u{E0061}\u{E0100}";

  await propose(injection);

  expect(sent[0]).toMatchObject({ allowed_mentions: { parse: [], replied_user: false }, flags: MessageFlags.SuppressEmbeds });
  const content = sent[0]?.content;
  expect(content?.match(/^```$/gm)).toHaveLength(2);
  expect(content).not.toContain("\u202E");
  expect(content).toContain("\\u{202e}\\u{e0061}\\u{e0100}");
  expect(content).toContain(`\n\`\`\`\nTo submit`);
  expect(content).toContain(`\`${confirm}\``);
  expect(drafts.consume(reply())?.endsWith(injection)).toBe(true);
});

test("rejects empty, raw oversize, and rendered oversize proposals without posting a partial draft", async () => {
  const { sent, propose } = fixture();

  await expect(propose(" \n")).rejects.toThrow("empty");
  await expect(propose("a".repeat(2_001))).rejects.toThrow("too long");
  await expect(propose("`".repeat(400))).rejects.toThrow("too long");
  expect(sent).toHaveLength(0);
});
