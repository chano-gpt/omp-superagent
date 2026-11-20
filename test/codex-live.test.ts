import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openCodexLive } from "./oracles/bun-gateway/codex-live";

const speech = Buffer.alloc(960).fill(Buffer.from([0x10, 0x10]));
const posted = Buffer.from([1, 0]);
const withheld = Buffer.from([2, 0]);
const dictated = 'src/GPUPath.ts에서 dispatch("keep_case")를 확인해 줘.\n테스트는 2개만 추가하고 설정은 바꾸지 마.';
const key = "OMP_SA_CODEX_VOICE_PYTHON";

test("a relative Codex profile is rejected instead of opening the default subscriber's login", async () => {
  const previous = process.env.CODEX_HOME;
  process.env.CODEX_HOME = "relative/profile";
  try {
    await expect(openCodexLive({ voice: "cove" }, {
      audio() {}, interrupt() {}, ended() {}, async forward() { throw new Error("unexpected handoff"); },
    }, new AbortController().signal)).rejects.toThrow("CODEX_HOME must be absolute");
  } finally {
    if (previous === undefined) delete process.env.CODEX_HOME;
    else process.env.CODEX_HOME = previous;
  }
});

const childSource = String.raw`
const send = event => process.stdout.write(JSON.stringify(event) + "\n");
const pcm = bytes => send({ type: "audio", audio: Buffer.from(bytes).toString("base64") });
const voice = process.argv.at(-1);
if (Object.keys(process.env).some(key => ["OPENAI_API_KEY", "DISCORD_BOT_TOKEN", "HTTPS_PROXY", "PYTHONPATH"].includes(key))) {
  send({ type: "ended" });
  process.exit(1);
}
if (voice === "breeze") {
  send({ type: "ended", detail: "private-provider-error" });
  process.exit(1);
}
send({ type: "ready" });
if (voice === "ember") send({ type: "draft", id: "no_speech", text: "Not a spoken request" });
const reader = require("node:readline").createInterface({ input: process.stdin });
let firstInput = true;
for await (const line of reader) {
  const event = JSON.parse(line);
  if (event.type === "stop") break;
  if (event.type === "input") {
    if (voice === "arbor") {
      send({ type: "audio", audio: "AQ==" });
      continue;
    }
    send({ type: "audio", audio: event.audio });
    if (firstInput && voice === "cove") {
      firstInput = false;
      const draft = { type: "draft", id: "owner_1", text: ${JSON.stringify(dictated)} };
      send(draft);
      send(draft);
      send({ type: "draft", id: "same_audio", text: "Do not propose twice for one utterance" });
    }
  }
  if (event.type === "draft_result") pcm([event.posted ? 1 : 2, 0]);
}
`;

function shellArg(path: string): string { return `'${path.replaceAll("'", `'\\''`)}'`; }

async function withFixture(run: (signal: AbortSignal) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "omp-codex-wire-"));
  const script = join(root, "wire-fixture.ts");
  const executable = join(root, "python");
  const saved = new Map([key, "OPENAI_API_KEY", "DISCORD_BOT_TOKEN", "HTTPS_PROXY", "PYTHONPATH"]
    .map(name => [name, process.env[name]]));
  const controller = new AbortController();
  try {
    await writeFile(script, childSource);
    await writeFile(executable,
      `#!/bin/sh\nexec ${shellArg(process.execPath)} --no-env-file ${shellArg(script)} "$@"\n`, { mode: 0o700 });
    for (const name of saved.keys()) process.env[name] = name === key ? executable : "test-secret";
    await run(controller.signal);
  } finally {
    controller.abort();
    for (const [name, value] of saved) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    await rm(root, { recursive: true, force: true });
  }
}

test("Codex voice relays PCM and posts one unchanged draft per voiced utterance", async () => {
  await withFixture(async signal => {
    const draftReceipt = Promise.withResolvers<void>();
    const proposed: string[] = [];
    const audio: Buffer[] = [];
    let interrupts = 0;
    const session = await openCodexLive({ voice: "cove" }, {
      audio(pcm) {
        audio.push(pcm);
        if (audio.some(frame => frame.equals(posted)) && audio.some(frame => frame.equals(withheld))) draftReceipt.resolve();
      },
      interrupt() { interrupts++; },
      ended() {},
      async forward(text) { proposed.push(text); return "Private Discord receipt is not sent to the bridge"; },
    }, signal);

    session.input(speech);
    await draftReceipt.promise;
    session.close();

    expect({ proposed, interrupts, speech: audio[0], receipts: audio.slice(1).sort(Buffer.compare) }).toEqual({
      proposed: [dictated], interrupts: 1, speech, receipts: [posted, withheld],
    });
  });
});

test("a bridge draft without owner speech is withheld", async () => {
  await withFixture(async signal => {
    const rejected = Promise.withResolvers<void>();
    const audio: Buffer[] = [];
    const drafts: string[] = [];
    await openCodexLive({ voice: "ember" }, {
      audio(pcm) {
        audio.push(pcm);
        if (pcm.equals(withheld)) rejected.resolve();
      },
      interrupt() {},
      ended() {},
      async forward(text) { drafts.push(text); return "not expected"; },
    }, signal);

    await rejected.promise;

    expect({ audio, drafts }).toEqual({ audio: [withheld], drafts: [] });
  });
});

test("an unavailable subscription never presents a ready session or discloses bridge diagnostics", async () => {
  await withFixture(async signal => {
    const opened = openCodexLive({ voice: "breeze" }, {
      audio() {},
      interrupt() {},
      ended() {},
      async forward() { return "not expected"; },
    }, signal);
    await expect(opened).rejects.toThrow("Codex subscription voice is unavailable or ended.");
  });
});

test("invalid PCM from a ready bridge terminates voice without emitting audio", async () => {
  await withFixture(async signal => {
    const ended = Promise.withResolvers<void>();
    const audio: Buffer[] = [];
    const session = await openCodexLive({ voice: "arbor" }, {
      audio(pcm) { audio.push(pcm); },
      interrupt() {},
      ended() { ended.resolve(); },
      async forward() { return "not expected"; },
    }, signal);

    session.input(speech);
    await ended.promise;

    expect(audio).toEqual([]);
  });
});
