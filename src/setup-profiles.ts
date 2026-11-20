import { readFileSync } from "node:fs";
import type { REST, RESTOptions } from "discord.js";
import type { Api } from "grammy";
import type { Config } from "./config";
import { validateCredential } from "./credentials";

const avatar = new URL("../assets/superagent-avatar.jpg", import.meta.url);

interface ProfileOptions {
  signal?: AbortSignal;
  createRest?: (options: Partial<RESTOptions>, token: string) => Pick<REST, "get" | "patch">;
  createApi?: (token: string) => Pick<Api, "getMe" | "setMyProfilePhoto" | "getUserProfilePhotos">;
}

interface ProfileResult {
  platform: "discord" | "telegram";
  status: "applied" | "failed";
  message: string;
}

/** One-shot profile REST calls only; never starts a messenger or native session. */
export async function setupBotProfiles(
  config: Config, credentials: Readonly<Record<string, string>>, options: ProfileOptions = {},
): Promise<ProfileResult[]> {
  const results: ProfileResult[] = [];
  if (!config.discord && !config.telegram) return results;
  const image = readFileSync(avatar);
  if (config.discord) {
    options.signal?.throwIfAborted();
    let attempted = false;
    try {
      const token = credentials[config.discord.tokenEnv]!;
      validateCredential(token);
      const { REST, Routes } = await import("discord.js");
      const rest = (options.createRest ?? ((settings, credential) => new REST(settings).setToken(credential)))(
        { version: "10", retries: 0, rejectOnRateLimit: () => true }, token,
      );
      const bot = await rest.get(Routes.user("@me"), { signal: options.signal }) as { id: string; bot?: boolean };
      if (bot.bot !== true || typeof bot.id !== "string" || !/^\d+$/.test(bot.id) || bot.id === config.discord.ownerId)
        throw new Error("Invalid bot identity");
      attempted = true;
      const updated = await rest.patch(Routes.user("@me"), {
        signal: options.signal,
        body: { avatar: `data:image/jpeg;base64,${image.toString("base64")}` },
      }) as { id: string; avatar?: string | null };
      if (updated.id !== bot.id || !updated.avatar) throw new Error("Missing profile receipt");
      const current = await rest.get(Routes.user("@me"), { signal: options.signal }) as { id: string; bot?: boolean; avatar?: string | null };
      if (current.id !== bot.id || current.bot !== true || current.avatar !== updated.avatar)
        throw new Error("Profile readback differs");
      results.push({ platform: "discord", status: "applied", message: "Mascot profile picture applied and verified by Discord profile readback." });
    } catch {
      results.push({ platform: "discord", status: "failed", message: attempted
        ? "Profile update is unresolved; it may have reached Discord. No automatic retry was attempted; API details suppressed."
        : "Profile inspection failed; no Discord profile update was attempted. Credential/API details suppressed." });
    }
  }
  if (config.telegram) {
    if (options.signal?.aborted) return results;
    let attempted = false;
    try {
      const token = credentials[config.telegram.tokenEnv]!;
      validateCredential(token);
      const { Api, InputFile } = await import("grammy");
      const { withTelegramSignal } = await import("./telegram-signal");
      const api = (options.createApi ?? (credential => new Api(credential, {
        sensitiveLogs: false, timeoutSeconds: 45, baseFetchConfig: { redirect: "error" },
      })))(token);
      const bot = await withTelegramSignal(options.signal, signal => api.getMe(signal));
      if (!bot.is_bot || !Number.isSafeInteger(bot.id) || bot.id <= 0 || String(bot.id) === config.telegram.ownerId)
        throw new Error("Invalid bot identity");
      attempted = true;
      if (await withTelegramSignal(options.signal, signal =>
        api.setMyProfilePhoto({ type: "static", photo: new InputFile(image, "superagent-avatar.jpg") }, signal)) !== true)
        throw new Error("Missing profile receipt");
      const current = await withTelegramSignal(options.signal, signal => api.getUserProfilePhotos(bot.id, { limit: 1 }, signal));
      if (current.total_count < 1 || !current.photos[0]?.some(photo => photo.file_id && photo.file_unique_id))
        throw new Error("Missing profile readback");
      results.push({ platform: "telegram", status: "applied", message: "Mascot upload accepted by Telegram; current profile photo confirmed by getUserProfilePhotos." });
    } catch {
      results.push({ platform: "telegram", status: "failed", message: attempted
        ? "Profile update is unresolved; it may have reached Telegram. No automatic retry was attempted; API details suppressed."
        : "Profile inspection failed; no Telegram profile update was attempted. Credential/API details suppressed." });
    }
  }
  return results;
}
