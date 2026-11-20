// rust-anything app replay test/rust-anything-gateway
// Original source: d5fb05934684b597dea2047ada55063da4635a91 (rust-anything 2.1.0).
// Original record: rust-anything app record test/rust-anything-gateway --no-traps \
//   --inputs test/rust-anything-gateway/inputs.jsonl --name gateway-fixture --timeout 25 -- \
//   bun --no-env-file --no-install ./probe.ts original /absolute/path/to/original-checkout
import { strict as assert } from "node:assert";
import { mkdtemp, realpath, rm } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { GatewayFixture, ordered } from "./fixture";
import { admission, approval, effects } from "./scenarios";

const scenarios = { admission, approval, effects };
export type Scenario = keyof typeof scenarios;

export function nativeCommand(): readonly string[] {
  const executable = process.env.OMP_SUPERAGENT_GATEWAY_DIFFERENTIAL_BIN ??
    join(import.meta.dir, "../../gateway/target/debug/examples/gateway-differential");
  assert.ok(isAbsolute(executable), "use an absolute Gateway fixture binary path");
  return [executable];
}

export async function observe(command: readonly string[], scenario: Scenario): Promise<unknown> {
  const root = await mkdtemp(join(await realpath("/tmp"), "ogd-"));
  const fixture = new GatewayFixture(root, command);
  try {
    try { return ordered(await scenarios[scenario](fixture)); }
    finally { await fixture.dispose(); }
  } catch (error) {
    // Child logs are private operator diagnostics, not the IPC response under
    // test. Keep them available on harness failures, including failed startup.
    const diagnostics = await fixture.diagnostics();
    if (diagnostics) console.error(diagnostics.trimEnd());
    throw error;
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

if (import.meta.main) {
  const [, , target, first, second] = process.argv;
  const original = target === "original";
  assert.ok(original || target === "native", "select original or native");
  const scenario = original ? second : first;
  assert.ok(scenario === "admission" || scenario === "approval" || scenario === "effects", "unknown scenario");
  if (original) assert.ok(first && isAbsolute(first), "provide an absolute original checkout path");
  const command = original
    ? [process.execPath, "--no-env-file", "--no-install", join(import.meta.dir, "original.ts"), String(first)]
    : nativeCommand();
  console.log(JSON.stringify(await observe(command, scenario), null, 2));
}
