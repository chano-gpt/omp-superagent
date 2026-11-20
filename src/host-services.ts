import { lstat } from "node:fs/promises";
import { once } from "node:events";
import { createConnection, isIPv4 } from "node:net";
import { FileLock } from "@oh-my-pi/pi-natives";
import { readOfflineUnits } from "@oh-my-pi/pi-coding-agent/cli/ps-data";
import { probeDaemon } from "@oh-my-pi/pi-coding-agent/daemon/client";
import { daemonEndpoint, daemonLockPath, daemonTcpAddress } from "@oh-my-pi/pi-coding-agent/daemon/paths";
import type { UnitSnapshot } from "@oh-my-pi/pi-coding-agent/daemon/protocol";
import type { ServiceUnit } from "./host-protocol";

/** An unreachable remote daemon cannot be inferred absent from this machine's store. */
async function confirmedOffline(): Promise<boolean> {
  if (FileLock.isHeld(daemonLockPath())) return false;
  const tcp = daemonTcpAddress();
  if (!tcp) return lstat(daemonEndpoint()).then(() => false, error => {
    if (error.code === "ENOENT") return true;
    throw error;
  });
  if (tcp.host !== "localhost" && tcp.host !== "::1" && !(isIPv4(tcp.host) && tcp.host.startsWith("127."))) return false;
  // probeDaemon also returns undefined on auth or protocol failures. Only a refused
  // loopback connection (not an accepting listener or a timeout) proves absence.
  const socket = createConnection({ ...tcp, signal: AbortSignal.timeout(1_000) });
  try { await once(socket, "connect"); return false; }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ECONNREFUSED"; }
  finally { socket.destroy(); }
}

/** Authoritative SDK inventory in the Host helper, after profile bootstrap. Never spawns or upgrades the daemon. */
export async function listServiceUnits(projectDir?: string, key?: string): Promise<ServiceUnit[]> {
  let units: UnitSnapshot[];
  const deadline = performance.now() + 2_000;
  for (;;) {
    const client = await probeDaemon();
    if (client) {
      try {
        units = (await client.request({ op: "list", kind: "proc",
          ...(projectDir === undefined ? {} : { scope: { kind: "project", dir: projectDir } as const }) })).units;
        break;
      } catch {
        // The daemon can exit between the handshake and list.
      } finally { client.close(); }
    }
    if (await confirmedOffline()) { units = await readOfflineUnits(); break; }
    // Stock takes its lifetime lock before it listens. Wait for that daemon,
    // never launch it or retry a mutation. An unknown endpoint stays fenced.
    if (!FileLock.isHeld(daemonLockPath()) || performance.now() >= deadline)
      throw new Error("Cannot confirm native daemon service ownership");
    await Bun.sleep(25);
  }
  return units.filter(unit => unit.kind === "proc" && (key === undefined || unit.key === key)
    && (projectDir === undefined || unit.scope.kind === "project" && unit.scope.dir === projectDir))
    .map(({ id, key, owner, generation, startedAt, restartCount, state, exitedAt, pids, supervised }) =>
      ({ id, key, owner, generation, startedAt, restartCount, state, exitedAt, pids, supervised }));
}
