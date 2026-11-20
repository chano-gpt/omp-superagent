import { Type } from "@sinclair/typebox/type";
import { Value } from "@sinclair/typebox/value";
import {
  ChannelType, MessageFlags, MessageReferenceType, MessageType, Routes,
  type Message, type RESTPostAPIChannelMessageJSONBody, type TextChannel,
} from "discord.js";
import { DeliveryError } from "../../../src/delivery.ts";

const CONFIRM = "/voice confirm";
export const VOICE_DRAFT_HEADER = "Proposed work from the live voice session (not yet submitted).\n";
const PROPOSAL_TTL_MS = 5 * 60 * 1000;
const MESSAGE_LIMIT = 2000;
const MAX_SNOWFLAKE = (1n << 64n) - 1n;

const receiptSchema = Type.Object({
  id: Type.String({ pattern: "^[1-9][0-9]{0,19}$" }),
  author: Type.Object({ id: Type.String(), bot: Type.Literal(true) }),
  channel_id: Type.String(),
  guild_id: Type.Optional(Type.String()),
  content: Type.String(),
  timestamp: Type.String({ pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})$" }),
});

type PostedDraft = {
  readonly id: string;
  readonly createdTimestamp: number;
  readonly accountId: string;
  readonly ownerId: string;
  readonly guildId: string;
  readonly channelId: string;
  readonly work: string;
};

type Draft = {
  readonly controller: AbortController;
  readonly signal: AbortSignal;
  readonly startedAt: number;
  readonly posted?: PostedDraft;
};

type DraftLifetime = {
  readonly signal: AbortSignal;
  readonly postDeadline?: number;
};

export class VoiceDrafts {
  private draft: Draft | null = null;
  private sending = false;

  /** The caller supplies the session's already allowlisted, bound guild text channel. */
  async propose(channel: TextChannel, ownerId: string, text: string, { signal, postDeadline }: DraftLifetime): Promise<void> {
    signal.throwIfAborted();
    if (this.sending || this.current()) {
      throw new DeliveryError("A voice work proposal is already outstanding.", "rejected");
    }
    const accountId = channel.client.user?.id;
    if (!accountId || !channel.guildId || channel.type !== ChannelType.GuildText) {
      throw new DeliveryError("Voice work proposals require a bound Discord guild text channel.", "rejected");
    }
    if (text.length > MESSAGE_LIMIT) {
      throw new DeliveryError("Voice work proposal is too long to review in one Discord message.", "rejected");
    }
    const work = text.trim();
    if (!work) throw new DeliveryError("Voice work proposal is empty.", "rejected");

    // A fenced literal cannot render links, custom emoji or Discord mentions.
    // Escape backticks, controls (including bidi overrides), and the escape
    // character itself so the displayed text cannot impersonate the instructions.
    const literal = work.replace(/[\\`\p{Cc}\p{Cf}\p{Cs}\p{Default_Ignorable_Code_Point}]/gu, character => character === "\n"
      ? character : `\\u{${character.codePointAt(0)!.toString(16)}}`);
    const content = VOICE_DRAFT_HEADER +
      `\`\`\`\n${literal}\n\`\`\`\n` +
      `To submit this proposal to HEAD, reply to this message with exactly \`${CONFIRM}\` ` +
      "while the voice session is active and within five minutes.\n" +
      "Check the transcribed request before confirming. HEAD replies only in Discord text. " +
      "Risky actions still require separate approval.";
    if (content.length > MESSAGE_LIMIT) {
      throw new DeliveryError("Voice work proposal is too long to review in one Discord message.", "rejected");
    }

    const controller = new AbortController();
    const draft = { controller, signal: AbortSignal.any([signal, controller.signal]), startedAt: Date.now() };
    const remaining = postDeadline === undefined ? undefined : Math.ceil(postDeadline - performance.now());
    if (remaining !== undefined && remaining <= 0) {
      throw new DeliveryError("The one-draft posting permission expired.", "rejected");
    }
    // The publication deadline must not shorten the confirmation window for
    // a card already accepted within the grant.
    const sendSignal = remaining === undefined ? draft.signal : AbortSignal.any([draft.signal, AbortSignal.timeout(remaining)]);
    const { guildId, id: channelId } = channel;
    const body = {
      content, allowed_mentions: { parse: [], replied_user: false }, flags: MessageFlags.SuppressEmbeds,
    } satisfies RESTPostAPIChannelMessageJSONBody;
    this.draft = draft;
    this.sending = true;
    try {
      // Unlike channel.send, the REST route accepts a signal for both the queue and the request.
      const sent = await channel.client.rest.post(Routes.channelMessages(channelId), {
        body, signal: sendSignal,
      }).catch(() => {
        // Even an abort may occur after transmission. Never infer a receipt or
        // retry; keep the slot fenced until expiry or revocation.
        throw new DeliveryError("Voice work proposal delivery is uncertain; it cannot be confirmed.", "uncertain");
      });
      if (this.current() !== draft) {
        throw new DeliveryError("The voice work proposal is no longer active and cannot be confirmed.", "rejected");
      }
      if (postDeadline !== undefined && performance.now() >= postDeadline) {
        throw new DeliveryError("The voice work proposal was not confirmed before posting permission expired.", "uncertain");
      }
      const unverified = () => new DeliveryError("Discord did not verify the voice work proposal; it cannot be confirmed.", "uncertain");
      // Create Message can omit guild_id: the authenticated, bound guild channel
      // and the exact channel_id still identify the destination.
      if (!Value.Check(receiptSchema, sent) || BigInt(sent.id) > MAX_SNOWFLAKE ||
        sent.author.id !== accountId || sent.channel_id !== channelId ||
        (sent.guild_id !== undefined && sent.guild_id !== guildId) || sent.content !== content) throw unverified();
      const createdTimestamp = Date.parse(sent.timestamp);
      if (!Number.isSafeInteger(createdTimestamp)) throw unverified();
      this.draft = {
        ...draft, startedAt: Date.now(),
        posted: { id: sent.id, createdTimestamp, accountId, ownerId, guildId, channelId, work },
      };
    } finally {
      this.sending = false;
    }
  }

  /** Call only for fresh MessageCreate events, never for updates or fetched history. */
  consume(message: Message): string | null {
    const draft = this.current()?.posted;
    if (!draft || message.content !== CONFIRM || message.type !== MessageType.Reply ||
      message.editedTimestamp !== null || message.webhookId !== null ||
      message.author.id !== draft.ownerId || message.author.bot || message.client.user?.id !== draft.accountId ||
      message.channelId !== draft.channelId || message.guildId !== draft.guildId ||
      message.id === draft.id || message.createdTimestamp < draft.createdTimestamp ||
      message.attachments.size || message.stickers.size || message.messageSnapshots.size) return null;

    const reference = message.reference;
    if (!reference || reference.messageId !== draft.id || reference.channelId !== draft.channelId ||
      (reference.guildId !== undefined && reference.guildId !== draft.guildId) ||
      (reference.type !== undefined && reference.type !== MessageReferenceType.Default)) return null;

    this.clear();
    return "Work proposed in a live Discord voice session and confirmed by the owner in writing.\n" +
      "This is an ordinary work request, not a slash command, native control, or approval. " +
      "Apply the usual safety and approval checks.\n\n" + draft.work;
  }

  clear(): void {
    const draft = this.draft;
    this.draft = null;
    draft?.controller.abort();
  }

  private current(): Draft | null {
    const draft = this.draft;
    if (!draft) return null;
    const elapsed = Date.now() - draft.startedAt;
    if (draft.signal.aborted || elapsed < 0 || elapsed >= PROPOSAL_TTL_MS) {
      this.clear();
      return null;
    }
    return draft;
  }
}
