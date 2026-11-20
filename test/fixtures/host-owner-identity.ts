import { dlopen } from "bun:ffi";
import { readFileSync, readlinkSync, realpathSync } from "node:fs";
import { hostOwnerExecutable } from "../../src/host-owner-path";

type FixtureOwnerIdentity = { readonly manifest: string; readonly start: string };

const darwin = process.platform === "darwin" ? dlopen("/usr/lib/libSystem.B.dylib", {
  proc_pidinfo: { args: ["i32", "i32", "u64", "ptr", "i32"], returns: "i32" },
  proc_pidpath: { args: ["i32", "ptr", "u32"], returns: "i32" },
  sysctl: { args: ["ptr", "u32", "ptr", "ptr", "ptr", "u64"], returns: "i32" },
}).symbols : undefined;

let executable: string | undefined;

/** Read kernel process identity, never the display name, environment or flat ps arguments. */
export function fixtureOwnerIdentity(pid: number): FixtureOwnerIdentity | undefined {
  executable ??= realpathSync(hostOwnerExecutable);
  const uid = process.getuid?.();
  if (uid === undefined) throw new Error("Host fixture custody requires a Unix UID");
  if (process.platform === "linux") {
    try {
      const stat = () => {
        const fields = readFileSync(`/proc/${pid}/stat`, "utf8");
        // The comm field can itself contain spaces and parentheses. Start time is field 22.
        return fields.slice(fields.lastIndexOf(") ") + 2).split(" ")[19];
      };
      const start = stat();
      if (!start || readlinkSync(`/proc/${pid}/exe`) !== executable) return;
      const uids = /^Uid:\s+(\d+)\s+(\d+)/m.exec(readFileSync(`/proc/${pid}/status`, "utf8"));
      if (Number(uids?.[1]) !== uid || Number(uids?.[2]) !== uid) return;
      const argv = readFileSync(`/proc/${pid}/cmdline`, "utf8").split("\0");
      const [argv0, manifest, terminator] = argv;
      if (argv.length === 3 && argv0 === hostOwnerExecutable && manifest && terminator === "" && stat() === start)
        return { manifest, start };
    } catch (error) {
      if (!(error instanceof Error && "code" in error
        && (error.code === "ENOENT" || error.code === "ESRCH" || error.code === "EACCES"))) throw error;
      // A PID can exit or change hands while /proc is being read.
    }
    return;
  }
  if (!darwin) throw new Error(`Host fixture custody is unsupported on ${process.platform}`);

  // XNU's 64-bit struct proc_bsdinfo, PROC_PIDTBSDINFO = 3.
  const info = Buffer.alloc(136);
  const readStart = () => {
    if (darwin.proc_pidinfo(pid, 3, 0n, info, info.length) !== info.length
      || info.readUInt32LE(20) !== uid || info.readUInt32LE(28) !== uid) return;
    return `${info.readBigUInt64LE(120)}-${info.readBigUInt64LE(128)}`;
  };
  const start = readStart();
  if (!start) return;
  const path = Buffer.alloc(4096);
  if (darwin.proc_pidpath(pid, path, path.length) <= 0
    || path.toString("utf8", 0, path.indexOf(0)) !== executable) return;

  // CTL_KERN/KERN_ARGMAX sizes the CTL_KERN/KERN_PROCARGS2 result. The
  // latter is argc, executable path, NUL padding, argv, then environment.
  const argmax = new Int32Array(1);
  const length = new BigUint64Array([4n]);
  if (darwin.sysctl(new Int32Array([1, 8]), 2, argmax, length, null, 0n) !== 0
    || argmax[0] < 4 || argmax[0] > 8 * 1024 * 1024) return;
  const argv = Buffer.alloc(argmax[0]);
  length[0] = BigInt(argv.length);
  if (darwin.sysctl(new Int32Array([1, 49, pid]), 3, argv, length, null, 0n) !== 0 || length[0] < 4n
    || argv.readInt32LE(0) !== 2) return;
  const end = Number(length[0]);
  const executableEnd = argv.indexOf(0, 4);
  if (executableEnd < 0) return;
  let argv0 = executableEnd + 1;
  while (argv0 < end && argv[argv0] === 0) argv0++;
  const argv0End = argv.indexOf(0, argv0);
  if (argv0End < 0 || argv.toString("utf8", argv0, argv0End) !== hostOwnerExecutable) return;
  const manifestEnd = argv.indexOf(0, argv0End + 1);
  if (manifestEnd <= argv0End + 1 || manifestEnd >= end || readStart() !== start) return;
  return { manifest: argv.toString("utf8", argv0End + 1, manifestEnd), start };
}
