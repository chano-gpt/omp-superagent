/** Isolated stock-stdio peer for Gateway lifecycle tests; no platform activation. */
import { appendFile, readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { readSessionHeaderId } from "@oh-my-pi/pi-coding-agent/session/session-loader";
import { loadConfig } from "../../src/config";
import { createGatewayMessenger } from "../../src/gateway-client";
import { Store } from "../../src/store";
import { recordHostFixture } from "./host-fixture-processes";
import { stockHostFixture } from "./stock-host";

const arg = (name: string) => process.argv[process.argv.indexOf(name) + 1]!;
const config = await loadConfig(process.env.OMP_SUPERAGENT_CONFIG!);
recordHostFixture(config.stateDir);
const sessionFile = arg("--session");
const sessionId = await readSessionHeaderId(sessionFile);
if (!sessionId) throw new Error("Lifecycle peer requires the actual materialized native header");
const messenger = createGatewayMessenger(config);
await appendFile(join(config.stateDir, "launches.jsonl"), `${JSON.stringify({ pid: process.pid,
  args: process.argv, config: JSON.parse(await readFile(arg("--config"), "utf8")),
  credentialPresent: Boolean(process.env.GATEWAY_TEST_BOT_TOKEN) })}\n`);
await stockHostFixture({
  sessionId, sessionFile,
  state: () => ({ isSettled: !existsSync(join(config.stateDir, "busy")) }),
  async activated() {
    // Like the real manager, attach only after the pipe owner is listening.
    // Native initialization/readback is covered by native-gateway.test.ts.
    const coordination = new Store(join(config.stateDir, "coordination.sqlite"));
    try {
      const saved = coordination.getMeta("head:preset-bootstrap");
      if (saved) {
        const initialization = JSON.parse(saved) as { sessionId: string; state: string };
        if (initialization.sessionId !== sessionId || initialization.state !== "complete")
          throw new Error("Lifecycle peer cannot replace an incomplete or foreign initialization receipt");
      } else coordination.setMeta("head:preset-bootstrap", JSON.stringify({ sessionId,
        state: "complete", replayBlocked: true, detail: "Lifecycle RPC peer: fixture presets were already verified." }));
    } finally { coordination.close(); }
    await messenger.start({
      message: async input => {
        await appendFile(join(config.stateDir, "received.jsonl"), `${JSON.stringify(input)}\n`);
        if (input.text !== "/restart") return;
        const store = new Store(join(config.stateDir, "coordination.sqlite"));
        let request: { requestId: string; inputId: string; ackOutputId: string };
        try { request = JSON.parse(store.getMeta("head:restart-fence")!); }
        finally { store.close(); }
        if (request.inputId !== input.id) throw new Error("Fixture restart source identity mismatch");
        await messenger.requestRestart(request);
      },
      answer: async () => {},
      restart: async notice => { await appendFile(join(config.stateDir, "restart-notices.jsonl"), `${JSON.stringify(notice)}\n`); },
    });
  },
  async shutdown(channel) {
    await appendFile(join(config.stateDir, "shutdowns.jsonl"), `${JSON.stringify({ pid: process.pid })}\n`);
    if (existsSync(join(config.stateDir, "ambiguous-shutdown"))) { channel.destroy(); return; }
    const busy = existsSync(join(config.stateDir, "busy")) || existsSync(join(config.stateDir, "busy-shutdown"));
    return busy ? { success: false, error: "Host is busy" } : { data: {} };
  },
  exit: () => messenger.stop(),
});
