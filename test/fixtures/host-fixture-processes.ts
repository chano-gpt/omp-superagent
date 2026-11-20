import { constants, existsSync, lstatSync, watch, writeFileSync } from "node:fs";
import { readdir, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { fixtureOwnerIdentity } from "./host-owner-identity";

const stopFile = "stop-test-hosts";
const prefix = "fixture-host-";
const launchName = /^\.launch-[^.]+$/;
const manifestFile = "owner.json";
type FixtureReceipt = { readonly launch: string; readonly nativePid: number; readonly ownerPid: number; readonly start: string };

function requirePrivateDirectory(directory: string): void {
  const info = lstatSync(directory);
  if (!info.isDirectory() || info.uid !== process.getuid?.() || (info.mode & 0o077) !== 0)
    throw new Error(`Host fixture requires a private owned directory: ${directory}`);
}

export function recordHostFixture(root: string): void {
  const config = process.env.OMP_SUPERAGENT_HOST_CONFIG;
  if (!config || basename(config) !== "config.json") throw new Error("Missing native Host fixture configuration");
  const directory = dirname(config);
  const launch = basename(directory);
  const parent = dirname(directory);
  if (!launchName.test(launch) || (parent !== root && parent !== join(root, "run")))
    throw new Error("Host fixture launch is outside its private root");
  requirePrivateDirectory(root);
  requirePrivateDirectory(parent);
  requirePrivateDirectory(directory);
  const manifest = join(directory, manifestFile);
  const ownerPid = process.ppid;
  const owner = ownerPid > 1 ? fixtureOwnerIdentity(ownerPid) : undefined;
  const info = lstatSync(manifest);
  if (owner?.manifest !== manifest || process.ppid !== ownerPid
    || !info.isFile() || info.uid !== process.getuid?.() || (info.mode & 0o077) !== 0)
    throw new Error(`Native Host fixture did not inherit its exact Rust pipe owner: ${directory}`);
  const receipt = join(root, `${prefix}${launch}.${process.pid}.${ownerPid}.${owner.start}`);
  if (existsSync(receipt)) return;
  // The filename is the complete receipt, so a writer cannot publish partial JSON.
  writeFileSync(receipt, "", { flag: "wx", mode: 0o600 });
  const stop = () => { if (existsSync(join(root, stopFile))) process.exit(1); };
  watch(root, { persistent: false }, stop);
  stop(); // Also covers teardown that began before registration.
}

async function receipts(root: string): Promise<FixtureReceipt[]> {
  const result: FixtureReceipt[] = [];
  for (const name of await readdir(root)) {
    if (!name.startsWith(prefix)) continue;
    const match = /^(\.launch-[^.]+)\.(\d+)\.(\d+)\.(\d+(?:-\d+)?)$/.exec(name.slice(prefix.length));
    const nativePid = Number(match?.[2]);
    const ownerPid = Number(match?.[3]);
    if (!match?.[1] || !match[4] || !Number.isSafeInteger(nativePid) || nativePid <= 1
      || !Number.isSafeInteger(ownerPid) || ownerPid <= 1 || ownerPid === nativePid)
      throw new Error(`Invalid fixture receipt ${name}; retained ${root}`);
    result.push({ launch: match[1], nativePid, ownerPid, start: match[4] });
  }
  return result;
}

export async function assertResidentFixtureOwner(root: string, identity: { readonly nativePid: number; readonly ownerPid: number }): Promise<void> {
  const { nativePid, ownerPid } = identity;
  const registration = (await receipts(root)).find(record => record.nativePid === nativePid && record.ownerPid === ownerPid);
  const live = fixtureOwnerIdentity(ownerPid);
  if (!registration || !live || live.start !== registration.start
    || (live.manifest !== join(root, registration.launch, manifestFile)
      && live.manifest !== join(root, "run", registration.launch, manifestFile)))
    throw new Error(`Native Host ${nativePid} is not served by the registered resident Rust owner ${ownerPid}; retained ${root}`);
}

function unregisteredOwners(runDirectory: string, launches: readonly string[]) {
  // A launch may fail before the native fixture runs. Only a private manifest
  // in this exact test run can identify such an owner; do not trust a process
  // name or an environment marker. An incomplete launch still fails the test.
  const manifests = new Set<string>();
  for (const launch of launches) {
    const manifest = join(runDirectory, launch, manifestFile);
    try {
      requirePrivateDirectory(dirname(manifest));
      const info = lstatSync(manifest);
      if (info.isFile() && info.uid === process.getuid?.() && (info.mode & 0o077) === 0) manifests.add(manifest);
    } catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
    }
  }
  if (!manifests.size) return [];
  const listing = Bun.spawnSync(["/bin/ps", "-u", String(process.getuid?.()), "-o", "pid="], { stderr: "pipe" });
  if (!listing.success) throw new Error(`Could not enumerate fixture owner PIDs in ${runDirectory}`);
  return listing.stdout.toString().trim().split(/\s+/).flatMap(value => {
    const pid = Number(value);
    if (!Number.isSafeInteger(pid) || pid <= 1) return [];
    const owner = fixtureOwnerIdentity(pid);
    return owner && manifests.has(owner.manifest) ? [{ pid, ...owner }] : [];
  });
}

export async function stopHostFixtures(root: string, runDirectory = root): Promise<number> {
  const { processAlive } = await import("../oracles/bun-gateway/gateway-restart");
  requirePrivateDirectory(root);
  if (runDirectory !== root && runDirectory !== join(root, "run"))
    throw new Error(`Host fixture run directory is outside ${root}`);
  // Native fixtures exit themselves. A surviving Rust pipe owner may be
  // intentionally fenced after an ambiguous exit; only terminate its exact
  // process generation after normal shutdown and native marker handling.
  await writeFile(join(root, stopFile), "", {
    mode: 0o600, flag: constants.O_WRONLY | constants.O_CREAT | constants.O_NOFOLLOW,
  });
  const began = performance.now();
  const deadline = began + 10_000;
  const signaled = new Map<string, { readonly firstSignal: number; readonly forced: boolean }>();
  const stopOwner = (target: { readonly pid: number; readonly manifest: string; readonly start: string }) => {
    const now = performance.now();
    const key = `${target.pid}.${target.start}`;
    const signal = signaled.get(key);
    if (now < began + 250 || (signal && (signal.forced || now < signal.firstSignal + 1_000))) return;
    const current = fixtureOwnerIdentity(target.pid);
    if (current?.start !== target.start || current.manifest !== target.manifest) return;
    try { process.kill(target.pid, signal ? "SIGKILL" : "SIGTERM"); }
    catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "ESRCH")) throw error;
    }
    signaled.set(key, { firstSignal: signal?.firstSignal ?? now, forced: signal !== undefined });
  };
  for (;;) {
    let directories: string[];
    try {
      requirePrivateDirectory(runDirectory);
      directories = await readdir(runDirectory);
    }
    catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
      directories = [];
    }
    const registrations = await receipts(root);
    const complete = new Set(registrations.map(record => record.launch));
    const launches = new Set([...complete, ...directories.filter(name => launchName.test(name))]);
    const incomplete = [...launches].filter(name => !complete.has(name));
    const live = new Set<number>();
    for (const { launch, nativePid, ownerPid, start } of registrations) {
      if (processAlive(nativePid)) live.add(nativePid);
      if (processAlive(ownerPid)) {
        live.add(ownerPid);
        stopOwner({ pid: ownerPid, manifest: join(runDirectory, launch, manifestFile), start });
      }
    }
    for (const target of unregisteredOwners(runDirectory, incomplete)) {
      live.add(target.pid);
      stopOwner(target);
    }
    if (!incomplete.length && !live.size) return launches.size;
    if (performance.now() >= deadline)
      throw new Error(`Fixture ${root} retained: incomplete launches [${incomplete}], live PIDs [${[...live]}]`);
    await Bun.sleep(25);
  }
}

export async function waitForFixtureProcessExit(pids: readonly number[]): Promise<void> {
  const { processAlive } = await import("../oracles/bun-gateway/gateway-restart");
  const deadline = performance.now() + 5_000;
  while (pids.some(processAlive)) {
    if (performance.now() >= deadline) throw new Error(`Fixture PIDs still running: ${pids.filter(processAlive)}`);
    await Bun.sleep(25);
  }
}
