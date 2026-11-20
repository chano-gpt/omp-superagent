import { readFile, readdir, realpath } from "node:fs/promises";
import { dirname, join } from "node:path";
import { FileLock, IpcConnection, ipcEndpoint } from "@oh-my-pi/pi-natives";
import { getDaemonDir, getDaemonRuntimeDir } from "@oh-my-pi/pi-utils/dirs";
import { hasFsCode } from "@oh-my-pi/pi-utils";
import { Type, type Static } from "@sinclair/typebox";
import { Value } from "@sinclair/typebox/value";
import type { ServiceUnit } from "./host-protocol";

// Stock OMP v18.22.2 launch/{broker,client,paths,protocol}. The stock ps client
// can start a broker when a connection fails; inventory here must never do so.
const snapshot = Type.Object({
  id: Type.String({ minLength: 1 }),
  name: Type.String({ minLength: 1 }),
  owner: Type.Optional(Type.String({ minLength: 1 })),
  state: Type.Union((["starting", "running", "ready", "restarting", "stopping", "exited", "failed"] as const)
    .map(state => Type.Literal(state))),
  pid: Type.Optional(Type.Integer({ minimum: 1 })),
  startedAt: Type.Number(),
  exitedAt: Type.Optional(Type.Number()),
  restartCount: Type.Integer({ minimum: 0 }),
});
const reply = Type.Union([
  Type.Object({ id: Type.String(), ok: Type.Literal(true), result: Type.Union([
    Type.Object({ op: Type.Literal("list"), daemons: Type.Array(snapshot) }),
    Type.Object({ op: Type.Literal("describe"), daemon: snapshot }),
  ]) }),
  Type.Object({ id: Type.String(), ok: Type.Literal(false), error: Type.String() }),
]);
const stored = Type.Object({ daemon: snapshot });

function unit(row: Static<typeof snapshot>, supervised: boolean): ServiceUnit {
  const { id, name, owner, state, pid, startedAt, exitedAt, restartCount } = row;
  return { id, key: name, state, pids: pid === undefined ? [] : [pid], startedAt, restartCount, supervised,
    ...(owner === undefined ? {} : { owner }), ...(exitedAt === undefined ? {} : { exitedAt }) };
}

async function directories(path: string): Promise<string[]> {
  try { return (await readdir(path, { withFileTypes: true })).filter(entry => entry.isDirectory()).map(entry => entry.name).sort(); }
  catch (error) {
    if (hasFsCode(error, "ENOENT")) return [];
    throw error;
  }
}

async function readScope(runtimeDir: string, key?: string): Promise<ServiceUnit[]> {
  const lease = join(runtimeDir, "broker.pid");
  const endpoint = ipcEndpoint(runtimeDir, "broker");
  let socket: IpcConnection;
  try { socket = await IpcConnection.connect(endpoint, { framing: "lines", timeoutMs: 500 }); }
  catch (error) {
    if (!(hasFsCode(error, "ENOENT") || hasFsCode(error, "ECONNREFUSED")) || FileLock.isHeld(lease))
      throw new Error("Cannot confirm native broker service ownership");
    const root = join(runtimeDir, "daemons");
    const offline = await Promise.all((await directories(root)).map(async name => {
      const record: unknown = JSON.parse(await readFile(join(root, name, "meta.json"), "utf8"));
      if (!Value.Check(stored, record)) throw new Error("Invalid persisted native broker service");
      // Keep the recorded state, never turn a stale running row into proof of an exit.
      return unit(record.daemon, false);
    }));
    if (FileLock.isHeld(lease)) throw new Error("Native broker started during service observation");
    return key === undefined ? offline : offline.filter(row => row.key === key);
  }
  try {
    if (!FileLock.isHeld(lease)) throw new Error("Native broker ownership lease is unavailable");
    const token = (await readFile(join(runtimeDir, "broker.token"), "utf8")).trim();
    if (!token) throw new Error("Native broker authentication is unavailable");
    const id = crypto.randomUUID();
    const { promise, resolve, reject } = Promise.withResolvers<ServiceUnit[]>();
    const timer = setTimeout(() => reject(new Error("Native broker inventory timed out")), 2_000);
    try {
      socket.start(line => {
        try {
          const response: unknown = JSON.parse(line);
          if (!Value.Check(reply, response) || response.id !== id || !FileLock.isHeld(lease))
            throw new Error("Native broker inventory was not confirmed");
          if (key === undefined && response.ok && response.result.op === "list") {
            resolve(response.result.daemons.map(row => unit(row, true)));
          } else if (key !== undefined && response.ok && response.result.op === "describe" && response.result.daemon.name === key) {
            resolve([unit(response.result.daemon, true)]);
          } else if (key !== undefined && !response.ok
            && (response.error === `Unknown daemon ${key}` || response.error.startsWith(`Unknown daemon ${key}. Available: `))) {
            resolve([]);
          } else throw new Error("Native broker inventory was not confirmed");
        } catch (error) { reject(error); }
      }, () => reject(new Error("Native broker closed before inventory was confirmed")));
      // The broker's list caps terminal rows. Named launch correlation needs
      // its exact row even if many other services have just finished.
      const operation = key === undefined ? { op: "list" } : { op: "describe", name: key };
      socket.send(JSON.stringify({ id, token, operation }));
      return await promise;
    } finally { clearTimeout(timer); }
  } finally { socket.destroy(); }
}

/** Use the exact native Host profile, without acquiring a broker lease. */
export function brokerServiceInventory(hostCwd: string) {
  const projects = dirname(getDaemonRuntimeDir(hostCwd));
  const globals = join(dirname(getDaemonDir()), "daemons", "global");
  return async (projectDir?: string, key?: string): Promise<ServiceUnit[]> => {
    if (projectDir !== undefined) return readScope(getDaemonRuntimeDir(projectDir), key);
    const [projectScopes, globalScopes] = await Promise.all([directories(projects), directories(globals)]);
    return (await Promise.all([
      ...projectScopes.filter(name => /^[0-9a-f]{16}$/.test(name)).map(name => readScope(join(projects, name))),
      ...globalScopes.map(async name => readScope(await realpath(join(globals, name)))),
    ])).flat();
  };
}
