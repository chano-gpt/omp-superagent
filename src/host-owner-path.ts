import { constants } from "node:fs";
import { access } from "node:fs/promises";
import { fileURLToPath } from "node:url";

export const hostOwnerExecutable = fileURLToPath(new URL(
  `../host-owner/bin/omp-host-owner-${process.platform}-${process.arch}`, import.meta.url,
));

export async function requireHostOwner(): Promise<string> {
  try { await access(hostOwnerExecutable, constants.X_OK); }
  catch (cause) {
    throw new Error(`Host owner missing or not executable at ${hostOwnerExecutable}; reinstall the platform package or run bun run build:owner in a source checkout`, { cause });
  }
  return hostOwnerExecutable;
}
