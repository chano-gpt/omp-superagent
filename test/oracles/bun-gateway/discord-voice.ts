import { ChannelType, MessageType, type Message, type TextChannel } from "discord.js";
import type { DiscordVoiceConfig } from "../../../src/config";
import { VoiceDrafts } from "./discord-voice-draft";
import type { openVoiceLink } from "./discord-voice-link";
import type { openLive } from "./openai-live";
import type { openCodexLive } from "./codex-live";

// Keep codecs and the provider out of setup, login, and ordinary text traffic.
const connectDiscord: typeof openVoiceLink = async (...args) =>
  (await import("./discord-voice-link")).openVoiceLink(...args);
const connectProvider: typeof openLive = async (...args) =>
  (await import("./openai-live")).openLive(...args);
const connectSubscription: typeof openCodexLive = async (...args) =>
  (await import("./codex-live")).openCodexLive(...args);

type DraftPermission = "off" | "call" | "one";

/** One explicitly started session on the existing Gateway client. No background rejoin or paid retries. */
export class DiscordVoice {
  private session?: AbortController;
  private readonly drafts = new VoiceDrafts();
  private ready = false;
  private permission: DraftPermission = "off";
  private shareUntil = 0;
  private lastCommand = 0n;
  private lastWorkOff = 0n;
  private lastDraftStart = 0n;
  private readonly startedAt = Date.now();

  constructor(private readonly config: DiscordVoiceConfig, private readonly owner: string,
    private readonly apiKey: string | undefined, private readonly report: (error: unknown) => void, private readonly link = connectDiscord,
    private readonly live = connectProvider, private readonly subscription = connectSubscription) {}

  static isControl(text: string): boolean { return /^\/voice(?:\s|$)/i.test(text.trim()); }
  static isPriorityControl(text: string): boolean { return /^\/voice (?:stop|work off|start (?:work|share))$/.test(text.trim()); }

  /** Only called for authorized, original MessageCreate events, never catchup or edits. */
  command(message: Message): string | null {
    if (message.author.bot || message.author.id !== this.owner || message.webhookId || message.editedTimestamp !== null ||
      (message.type !== MessageType.Default && message.type !== MessageType.Reply) || message.channel.type !== ChannelType.GuildText ||
      message.channelId !== this.config.textChannelId || message.createdTimestamp < this.startedAt ||
      !/^\d{1,20}$/.test(message.id)) return null;
    const control = message.content.trim();
    if (control !== "/voice confirm" && (message.type !== MessageType.Default || message.reference ||
      message.attachments.size || message.stickers.size || message.messageSnapshots.size)) return null;
    const sequence = BigInt(message.id);
    // Work-off revokes earlier permissions and confirmations, but must not
    // discard an ordinary conversation start still queued behind text intake.
    if (sequence <= this.lastCommand || (control !== "/voice start" && sequence <= this.lastWorkOff)) return null;
    const startWithDrafts = control === "/voice start work" || control === "/voice start share";
    const start = control === "/voice start" || startWithDrafts;
    const channel = message.channel;
    if (startWithDrafts) {
      // A refused priority start is still one-shot, but cannot advance the
      // watermark of an earlier, valid confirmation in the ordinary queue.
      if (sequence <= this.lastDraftStart) return null;
      this.lastDraftStart = sequence;
    }
    if (start && this.session) {
      if (!startWithDrafts) this.lastCommand = sequence;
      this.notice(channel, "A voice session is already starting or active.");
      return null;
    }
    if (control === "/voice start share" && this.config.auth !== "subscription") {
      this.notice(channel, "/voice start share is only available in subscription mode. Use /voice start work for API voice drafts.");
      return null;
    }
    if (control === "/voice work off") this.lastWorkOff = sequence;
    else this.lastCommand = sequence;
    if (control === "/voice confirm") {
      const work = this.drafts.consume(message);
      if (!work) this.notice(channel, "No matching active voice draft. Reply to the current draft, or write the request directly.");
      return work;
    }
    if (control === "/voice stop") {
      this.close();
      this.notice(channel, "Voice is off.");
    } else if (control === "/voice work off") {
      this.revokeWork();
      this.notice(channel, "Voice work drafts are off. Unconfirmed drafts can no longer be submitted.");
    } else if (start) {
      const permission = control === "/voice start work" ? "call" : control === "/voice start share" ? "one" : "off";
      void this.start(channel, permission).catch(() => this.report(new Error("Discord voice startup failed")));
    } else this.notice(channel, this.config.auth === "subscription"
      ? "Use /voice start for conversation, /voice start work for work drafts, or /voice start share for one draft. Use /voice work off or /voice stop to revoke."
      : "Use /voice start for conversation or /voice start work for work drafts. Use /voice work off or /voice stop to revoke. Voice uses paid OpenAI Live audio.");
    return null;
  }

  private async start(text: TextChannel, permission: DraftPermission): Promise<void> {
    const session = new AbortController();
    this.session = session;
    // A mode is selected before this provider receives any audio. It can only
    // be revoked, not granted to audio or a delegation from an existing call.
    this.permission = permission;
    const { signal } = session;
    const finish = (reason: string) => {
      if (this.session !== session) return;
      this.close();
      this.notice(text, reason);
    };
    const timer = setTimeout(() => finish("Voice stopped at the configured session time limit."),
      this.config.maxSessionSeconds * 1000);
    signal.addEventListener("abort", () => clearTimeout(timer), { once: true });
    try {
      const config = this.config;
      let connect: (handlers: Parameters<typeof openLive>[1]) => ReturnType<typeof openLive>;
      if (config.auth === "subscription") {
        connect = handlers => this.subscription({ voice: config.voice }, handlers, signal);
      } else {
        const apiKey = this.apiKey;
        if (!apiKey) { finish("A separate voice provider key is required. No Live session was opened."); return; }
        connect = handlers => this.live({ model: config.model, backendModel: config.backendModel, voice: config.voice, apiKey }, handlers, signal);
      }
      const channel = await text.guild.channels.fetch(this.config.channelId);
      if (signal.aborted) return;
      if (channel?.type !== ChannelType.GuildVoice || channel.guildId !== text.guildId ||
        text.guild.voiceStates.cache.get(this.owner)?.channelId !== channel.id) {
        finish("Join the configured voice channel in this server before starting voice."); return;
      }
      let provider: Awaited<ReturnType<typeof openLive>> | undefined;
      const ended = () => finish("Voice session ended. Use /voice start to open a new session.");
      const activeLink = await this.link(channel, this.owner, {
        audio: pcm => { if (!signal.aborted) provider?.input(pcm); },
        ended,
      }, signal);
      if (signal.aborted) { activeLink.close(); return; }
      provider = await connect({
        audio: pcm => { if (!signal.aborted) activeLink.play(pcm); },
        interrupt: () => activeLink.clear(), ended,
        forward: async request => {
          signal.throwIfAborted();
          const postDeadline = this.permission === "one" ? this.shareUntil : undefined;
          if (!this.ready || (this.permission !== "call" && (postDeadline === undefined || performance.now() >= postDeadline))) {
            throw new Error("No written permission to share a voice draft");
          }
          if (this.permission === "one") this.permission = "off";
          await this.drafts.propose(text, this.owner, request, { signal, postDeadline });
          return "Draft posted for written confirmation. Not yet submitted to HEAD or executed.";
        },
      });
      if (signal.aborted) { provider.close(); return; }
      if (this.permission === "one") this.shareUntil = performance.now() + 60_000;
      this.ready = true;
      const providerName = config.auth === "subscription" ? "OpenAI via your Codex subscription" : "OpenAI Live";
      const drafts = this.permission === "call"
        ? "Explicit work requests can be posted here as drafts for this call."
        : this.permission === "one" ? "One work draft can be posted here in the next minute."
          : "Draft posting is off. To enable it, stop this call and use /voice start work.";
      this.notice(text, `Voice ready. Audio goes to ${providerName}. ${drafts} Use /voice stop to disconnect.`);
    } catch {
      // Never send SDK errors, bearer headers, owner audio, or transcripts to Discord/logs.
      finish(this.config.auth === "subscription"
        ? "Could not open Codex subscription voice. Check the voice bridge, subscription login, voice access and Discord channel. No API fallback."
        : "Could not open voice. Check channel membership, permissions, voice dependencies and provider settings. No automatic retry.");
    }
  }

  private notice(channel: TextChannel, content: string): void {
    void channel.send({ content, allowedMentions: { parse: [] } }).catch(() => this.report(new Error("Discord voice notice delivery failed")));
  }

  private revokeWork(): void {
    this.permission = "off";
    this.shareUntil = 0;
    this.drafts.clear();
  }

  close(): void {
    const session = this.session;
    this.session = undefined;
    this.ready = false;
    this.revokeWork();
    session?.abort();
  }
}
