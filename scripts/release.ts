#!/usr/bin/env bun
import { constants } from "node:fs";
import { access, mkdir, mkdtempDisposable } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dir, "..");
const [mode, ...extra] = process.argv.slice(2);
if (extra.length || mode !== undefined && mode !== "--publish" && mode !== "--replace")
  throw new Error("Usage: bun run release [--publish|--replace]");

async function run(command: string[]): Promise<string> {
  const child = Bun.spawn(command, { cwd: root, stdin: "ignore", stdout: "pipe", stderr: "inherit" });
  const text = await new Response(child.stdout).text();
  if (await child.exited !== 0) throw new Error(`Command failed: ${command.join(" ")}`);
  return text.trim();
}

if (await run(["git", "status", "--porcelain"]))
  throw new Error("Release packaging requires a clean checkout; commit the intended release changes first");
const { name, version } = JSON.parse(await run(["git", "show", "HEAD:package.json"])) as { name: string; version: string };
const tag = `v${version}`;
const revision = await run(["git", "rev-parse", "HEAD"]);
if (mode !== undefined) {
  if (process.env.GITHUB_REF_TYPE !== "tag" || process.env.GITHUB_REF_NAME !== tag)
    throw new Error(`Publishing requires the ${tag} tag workflow`);
  if (await run(["git", "rev-parse", `${tag}^{commit}`]) !== revision)
    throw new Error("Release tag does not identify the checked-out commit");
}
if (mode === "--replace" && process.env.GITHUB_EVENT_NAME !== "workflow_dispatch")
  throw new Error("Replacing a release requires the manual tag workflow");
const notes = join(root, "docs/releases", `${version}.md`);
if (!await Bun.file(notes).exists()) throw new Error(`Missing release notes: ${notes}`);
const directory = join(root, "dist");
await mkdir(directory, { recursive: true });
const sourceFilename = `${name}-${version}.tar.gz`;
const packageFilename = `${name}-${version}.tgz`;
const nativePlatforms = ["darwin-arm64", "darwin-x64", "linux-arm64", "linux-x64"] as const;
const nativePackages = [
  { directory: "host-owner", binary: "omp-host-owner" },
  { directory: "gateway", binary: "omp-gateway" },
] as const;

async function verifyNativePayload(directory: string): Promise<void> {
  for (const pkg of nativePackages) {
    const binDir = join(directory, pkg.directory, "bin");
    for (const platform of nativePlatforms) {
      await access(join(binDir, `${pkg.binary}-${platform}`), constants.X_OK);
      const notices = join(binDir, "notices", platform);
      await access(join(notices, "Cargo-LICENSES.txt"));
      await access(join(notices, "Cargo-SOURCES.txt"));
      await access(join(notices, "rust", "COPYRIGHT-library.html"));
      await access(join(notices, "rust", "licenses"));
    }
  }
}

if (mode === undefined) {
  // Each target's CI runner builds both packages with --locked; the archive
  // job assembles those same outputs instead of rebuilding any one target.
  await verifyNativePayload(root);
  const archive = join(directory, sourceFilename);
  const sourcePrefix = `${name}-${version}/`;
  await run(["git", "archive", "--format=tar.gz", `--prefix=${sourcePrefix}`, `--output=${archive}`, revision]);
  const sources = new Set((await run(["tar", "-tzf", archive])).split("\n"));
  if (!sources.has(`${sourcePrefix}.cargo/config.toml`)) throw new Error("Source archive lacks .cargo/config.toml");
  for (const pkg of nativePackages) {
    const prefix = `${sourcePrefix}${pkg.directory}/`;
    for (const file of ["Cargo.toml", "Cargo.lock", "src/main.rs"]) {
      if (!sources.has(`${prefix}${file}`)) throw new Error(`Source archive lacks ${pkg.directory}/${file}`);
    }
    if ([...sources].some(file => file.startsWith(`${prefix}target/`) || file.startsWith(`${prefix}bin/`))) {
      throw new Error(`The source archive contains generated ${pkg.directory} artifacts`);
    }
  }
  const npmArchive = join(directory, packageFilename);
  await run([process.execPath, "pm", "pack", "--ignore-scripts", "--filename", npmArchive]);
  const files = (await run(["tar", "-tzf", npmArchive])).split("\n");
  for (const pkg of nativePackages) {
    const prefix = `package/${pkg.directory}/`;
    if (files.some(file => file.startsWith(prefix) && file !== prefix && !file.startsWith(`${prefix}bin/`))) {
      throw new Error(`Unexpected ${pkg.directory} build or source data in the npm tarball`);
    }
  }
  // Packlist checks alone cannot detect a packer that strips executable bits.
  await using unpacked = await mkdtempDisposable(join(tmpdir(), "omp-sa-pack-"));
  await run(["tar", "-xzf", npmArchive, "-C", unpacked.path]);
  await verifyNativePayload(join(unpacked.path, "package"));
}

const assets: string[] = [];
for (const filename of [sourceFilename, packageFilename]) {
  const path = join(directory, filename);
  const digest = new Bun.CryptoHasher("sha256").update(await Bun.file(path).arrayBuffer()).digest("hex");
  const checksum = `${path}.sha256`;
  const contents = `${digest}  ${filename}\n`;
  if (mode === undefined) await Bun.write(checksum, contents);
  else if (await Bun.file(checksum).text() !== contents) throw new Error(`Packaged asset checksum mismatch: ${filename}`);
  assets.push(path, checksum);
}
console.log(`${mode === undefined ? "Packaged" : "Verified staged release for"} ${revision}\n${assets.join("\n")}`);
if (mode === "--publish") {
  console.log(await run(["gh", "release", "create", tag, ...assets,
    "--verify-tag", "--title", `${name} ${version}`, "--notes-file", notes]));
} else if (mode === "--replace") {
  // Check the live tag, not just the checkout used by earlier CI jobs.
  const remote = await run(["git", "ls-remote", "origin", `refs/tags/${tag}`, `refs/tags/${tag}^{}`]);
  if (remote.split("\n").at(-1)?.split("\t")[0] !== revision)
    throw new Error("Remote release tag no longer identifies the verified commit");
  console.log(await run(["gh", "release", "upload", tag, ...assets, "--clobber"]));
  console.log(await run(["gh", "release", "edit", tag, "--verify-tag", "--notes-file", notes]));
}
