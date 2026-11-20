import { expect, test } from "bun:test";
import { join } from "node:path";
import { nativeCommand, observe, type Scenario } from "./rust-anything-gateway/probe";

const differential = process.env.OMP_SUPERAGENT_GATEWAY_DIFFERENTIAL === "1" ? test : test.skip;
const cases = [
  ["admission", "initial-readiness-and-ordered-replay"],
  ["approval", "approval-reconciliation-and-closure"],
  ["effects", "completed-and-lost-effects"],
] as const satisfies readonly (readonly [Scenario, string])[];

for (const [scenario, name] of cases) {
  differential(`native Gateway matches the recorded original: ${name}`, async () => {
    const original = Bun.file(join(import.meta.dir, "rust-anything-gateway/rust/app-cases", `${name}.stdout`));
    const expected: unknown = await original.json();
    expect(await observe(nativeCommand(), scenario)).toEqual(expected);
  }, 30_000);
}
