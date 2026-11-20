import { strict as assert } from "node:assert";
import { rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { stockHostFixture } from "../fixtures/stock-host";

const { FIXTURE_ROOT: root, FIXTURE_MODE: mode, FIXTURE_SESSION: sessionFile } = process.env;
assert.ok(root && sessionFile, "synthetic peer requires an isolated fixture root and session");
assert.ok(mode === "clean" || mode === "child-nonzero", "unknown fixture mode");
setTimeout(() => process.exit(87), 16_000).unref();
const instanceKey = crypto.randomUUID();
let protocolVersion = 1;
let askDialog = false;
let busy = false;
await stockHostFixture({
  sessionFile,
  state: () => ({
    instanceKey,
    isSettled: !busy,
    isStreaming: busy,
    fixtureAskDialog: askDialog,
    fixtureNativeProtocol: protocolVersion,
  }),
  handle(command, channel) {
    switch (command.type) {
      case "negotiate_protocol":
        assert.equal(command.protocolVersion, 2);
        protocolVersion = 2;
        channel.encoder.setProtocolVersion(protocolVersion);
        return { data: { protocolVersion } };
      case "set_ask_dialog":
        assert.equal(command.enabled, true);
        askDialog = true;
        return { data: { enabled: askDialog } };
      case "fixture_set_busy":
        assert.equal(typeof command.busy, "boolean");
        busy = command.busy === true;
        return { data: { busy } };
      case "get_fixture_large": {
        // Simulate a real v2 event, not an invalid >1 MiB unchunked stock line.
        const event = { type: "fixture_large", unicode: "가🙂? ".repeat(145_000) };
        for (const physical of channel.encoder.encodeFrames(event)) channel.output.write(physical);
        return { data: { emitted: true } };
      }
      default:
        return undefined;
    }
  },
  async activated() {
    const temporary = join(root, "activation.tmp");
    await writeFile(temporary, JSON.stringify({ nativePid: process.pid, instanceKey }), { flag: "wx", mode: 0o600 });
    await rename(temporary, join(root, "activation"));
  },
  exit: async () => { if (mode === "child-nonzero") process.exit(23); },
});
