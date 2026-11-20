import { existsSync, watch, writeFileSync } from "node:fs";
import { readdir, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const stopFile = "stop-test-hosts";
const prefix = "fixture-host-";

// Installed only in fresh test working directories. No extra process, global
// Bun setting or production shutdown command is needed for an ambiguous exit.
export async function prepareHostFixture(cwd: string): Promise<void> {
  await writeFile(join(cwd, "bunfig.toml"), `preload = [${JSON.stringify(fileURLToPath(import.meta.url))}]\n`,
    { mode: 0o600, flag: "wx" });
}

function register(root: string, directory: string, role: "native" | "owner"): void {
  const launch = basename(directory);
  if (!launch.startsWith(".launch-") || (dirname(directory) !== root && dirname(directory) !== join(root, "run")))
    throw new Error("Host fixture launch is outside its private root");
  const receipt = join(root, `${prefix}${launch}.${role}.${process.pid}`);
  if (existsSync(receipt)) return;
  // The filename is the complete receipt, so a writer cannot publish partial JSON.
  writeFileSync(receipt, "", { flag: "wx", mode: 0o600 });
  const stop = () => { if (existsSync(join(root, stopFile))) process.exit(1); };
  watch(root, { persistent: false }, stop);
  stop(); // Also covers teardown that began before registration.
}

if (process.argv[1] === fileURLToPath(new URL("../../src/host-owner.ts", import.meta.url))) {
  const manifest = process.argv[2];
  if (!manifest || basename(manifest) !== "owner.json") throw new Error("Missing Host fixture manifest");
  const directory = dirname(manifest);
  const parent = dirname(directory);
  register(basename(parent) === "run" ? dirname(parent) : parent, directory, "owner");
}

export function recordHostFixture(root: string): void {
  const config = process.env.OMP_SUPERAGENT_HOST_CONFIG;
  if (!config) throw new Error("Missing native Host fixture configuration");
  register(root, dirname(config), "native");
}

export async function stopHostFixtures(root: string, runDirectory = root): Promise<number> {
  const { processAlive } = await import("../../src/gateway-restart");
  // Normal shutdown is exercised first by the fixture. Any remaining process
  // exits itself: a reused numeric PID can never receive a termination signal.
  await writeFile(join(root, stopFile), "", { mode: 0o600 });
  const deadline = performance.now() + 10_000;
  for (;;) {
    let directories: string[];
    try { directories = await readdir(runDirectory); }
    catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
      directories = [];
    }
    const owners = new Set<string>();
    const natives = new Set<string>();
    const live: number[] = [];
    for (const name of await readdir(root)) {
      if (!name.startsWith(prefix)) continue;
      const match = /^(\.launch-[^.]+)\.(owner|native)\.(\d+)$/.exec(name.slice(prefix.length));
      const pid = Number(match?.[3]);
      if (!match?.[1] || !Number.isSafeInteger(pid) || pid <= 1)
        throw new Error(`Invalid fixture receipt ${name}; retained ${root}`);
      (match[2] === "owner" ? owners : natives).add(match[1]);
      if (processAlive(pid)) live.push(pid);
    }
    const launches = new Set([...owners, ...natives, ...directories.filter(name => name.startsWith(".launch-"))]);
    const incomplete = [...launches].filter(name => !owners.has(name) || !natives.has(name));
    if (!incomplete.length && !live.length) return launches.size;
    if (performance.now() >= deadline)
      throw new Error(`Fixture ${root} retained: incomplete launches [${incomplete}], live PIDs [${live}]`);
    await Bun.sleep(25);
  }
}

export async function waitForFixtureProcessExit(pids: readonly number[]): Promise<void> {
  const { processAlive } = await import("../../src/gateway-restart");
  const deadline = performance.now() + 5_000;
  while (pids.some(processAlive)) {
    if (performance.now() >= deadline) throw new Error(`Fixture PIDs still running: ${pids.filter(processAlive)}`);
    await Bun.sleep(25);
  }
}
