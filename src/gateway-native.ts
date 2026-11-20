import { Database } from "bun:sqlite";
import { constants } from "node:fs";
import { access } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import type { Config } from "./config";
import type { GatewayRestartRecord } from "./contracts";
import { DeliveryError } from "./delivery";
import { gatewayCall, gatewayPaths, gatewayStatus } from "./gateway-protocol";

const root = fileURLToPath(new URL("../", import.meta.url));
const binary = join(root, "gateway", "bin", `omp-gateway-${process.platform}-${process.arch}`);

export async function requireGateway(): Promise<string> {
  await access(binary, constants.X_OK).catch((cause: unknown) => {
    throw new Error(`Native Gateway is missing or not executable: ${binary}; reinstall the platform package or build the native binaries`, { cause });
  });
  return binary;
}

/** The detached native launcher decides ownership before returning success.
 * Read back its exact PID over the existing private operator protocol. */
export async function launchGateway(config: Config) {
  const child = Bun.spawn([await requireGateway(), "launch", "--config", config.path], {
    env: { ...process.env, OMP_SUPERAGENT_ROOT: root, OMP_SUPERAGENT_BUN: process.execPath },
    stdin: "ignore", stdout: "pipe", stderr: "inherit",
  });
  const [stdout, code] = await Promise.all([new Response(child.stdout).text(), child.exited]);
  if (code !== 0) throw new Error(`Gateway startup not confirmed; inspect status and ${gatewayPaths(config).logPath}`);
  const report: unknown = JSON.parse(stdout);
  const status = await gatewayStatus(config);
  if (!status || typeof report !== "object" || report === null || !("gateway" in report) ||
      typeof report.gateway !== "object" || report.gateway === null || !("pid" in report.gateway) ||
      report.gateway.pid !== status.gateway.pid) throw new Error("Gateway launch receipt does not match live ownership");
  return status;
}

export async function stopGateway(config: Config): Promise<void> {
  try { await gatewayCall(gatewayPaths(config).socketPath, "/stop", {}, { timeoutMs: 25_000 }); }
  catch (error) {
    const db = new Database(join(config.stateDir, "coordination.sqlite"), { readonly: true });
    try {
      const row = db.query<{value: string}, []>("SELECT value FROM metadata WHERE key='gateway:restart'").get();
      const restart: GatewayRestartRecord | null = row ? JSON.parse(row.value) : null;
      if (restart && (restart.state === "accepted" || restart.state === "head-stopped"))
        throw new DeliveryError("Gateway stop was not confirmed during a restart handoff; the successor was not cancelled. Inspect status before retrying stop.", "uncertain");
    } finally { db.close(); }
    throw error;
  }
}
