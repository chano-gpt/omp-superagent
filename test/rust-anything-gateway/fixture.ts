import { strict as assert } from "node:assert";
import { lstat, readFile } from "node:fs/promises";
import { request, type IncomingMessage } from "node:http";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { Readable } from "node:stream";

const TIME_LIMIT = 5_000;

export function record(value: unknown): Record<string, unknown> {
  assert.ok(value && typeof value === "object" && !Array.isArray(value), "expected a structured result");
  return Object.fromEntries(Object.entries(value));
}

export function ordered(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(ordered);
  if (value && typeof value === "object") return Object.fromEntries(
    Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, field]) => [key, ordered(field)]));
  return value;
}

async function within<T>(work: Promise<T>, label: string): Promise<T> {
  const expired = Promise.withResolvers<never>();
  const timer = setTimeout(() => expired.reject(new Error(`${label} timed out`)), TIME_LIMIT);
  try { return await Promise.race([expired.promise, work]); }
  finally { clearTimeout(timer); }
}

class Lines {
  private readonly parser;
  private readonly lines;
  constructor(input: Readable) {
    this.parser = createInterface({ input, crlfDelay: Infinity });
    this.lines = this.parser[Symbol.asyncIterator]();
  }
  async next(label: string): Promise<Record<string, unknown>> {
    const result = await within(this.lines.next(), label);
    assert.ok(!result.done, `${label}: unexpected EOF`);
    return record(JSON.parse(result.value));
  }
  close(): void { this.parser.close(); }
}

export const HEAD = "a8e21f03-fcb5-47ba-8b87-bcbaf08bd3de";
export const REPLACEMENT = "b8e21f03-fcb5-47ba-8b87-bcbaf08bd3de";
const headPid = process.pid;

export class GatewayFixture {
  private readonly token = crypto.randomUUID();
  private readonly children = new Set<FixtureProcess>();
  constructor(readonly root: string, private readonly command: readonly string[]) {}

  async start(generation: number): Promise<FixtureProcess> {
    const socketPath = join(this.root, `g${generation}.sock`);
    const child = Bun.spawn([...this.command, this.root, socketPath, String(headPid)], {
      cwd: this.root, stdin: "pipe", stdout: "pipe", stderr: "pipe",
      env: {
        PATH: "/usr/bin:/bin", HOME: this.root, USERPROFILE: this.root, TMPDIR: this.root,
        XDG_CONFIG_HOME: this.root, XDG_DATA_HOME: this.root, XDG_CACHE_HOME: this.root,
        XDG_STATE_HOME: this.root, PI_CODING_AGENT_DIR: this.root,
        FIXTURE_GATEWAY_TOKEN: this.token,
      },
    });
    const process = new FixtureProcess(child, { socketPath, token: this.token });
    this.children.add(process);
    assert.deepEqual(await process.announcement("Gateway socket startup"), { started: true });
    const stat = await lstat(socketPath);
    assert.equal(stat.isSocket(), true);
    assert.equal(stat.mode & 0o777, 0o600);
    return process;
  }

  async journal(name: "activity" | "remote"): Promise<Record<string, unknown>[]> {
    const text = await readFile(join(this.root, `${name}.jsonl`), "utf8");
    return text.trimEnd().split("\n").map(line => record(JSON.parse(line)));
  }

  async dispose(): Promise<void> {
    for (const child of this.children) await child.dispose();
  }

  async diagnostics(): Promise<string> {
    return (await Promise.all(Array.from(this.children, child => child.diagnostics))).join("");
  }
}

export class FixtureProcess {
  readonly diagnostics: Promise<string>;
  private readonly controls: Lines;
  private readonly streams = new Set<IncomingMessage>();

  constructor(
    private readonly child: Bun.Subprocess<"pipe", "pipe", "pipe">,
    private readonly endpoint: { readonly socketPath: string; readonly token: string },
  ) {
    this.controls = new Lines(Readable.from(child.stdout));
    this.diagnostics = new Response(child.stderr).text();
  }

  announcement(label: string) { return this.controls.next(label); }

  private async write(command: object): Promise<void> {
    this.child.stdin.write(`${JSON.stringify(command)}\n`);
    await this.child.stdin.flush();
  }

  async beginAdmission(event: object): Promise<() => Promise<Record<string, unknown>>> {
    await this.write({ command: "admit", event });
    assert.deepEqual(await this.announcement("adapter input reached readiness gate"), { waiting: true });
    return () => this.announcement("durable adapter admission");
  }

  async admit(event: object) { return (await this.beginAdmission(event))(); }

  async hasAnswer(id: string, route: object) {
    await this.write({ command: "has-answer", id, route });
    return this.announcement("pending answer lookup");
  }

  private open(path: string, options: {
    readonly client?: string;
    readonly body?: object;
    readonly unauthenticated?: boolean;
  }): Promise<IncomingMessage> {
    const data = options.body === undefined ? undefined : Buffer.from(JSON.stringify(options.body));
    const req = Promise.withResolvers<IncomingMessage>();
    const connection = request({
      path, socketPath: this.endpoint.socketPath, agent: false, method: data === undefined ? "GET" : "POST",
      headers: {
        ...(options.unauthenticated ? {} : { authorization: `Bearer ${this.endpoint.token}` }),
        ...(options.client === undefined ? {} : { "x-gateway-client": options.client }),
        ...(data === undefined ? {} : { "content-type": "application/json", "content-length": data.length }),
      },
    }, response => { clearTimeout(deadline); req.resolve(response); });
    const deadline = setTimeout(() => connection.destroy(new Error(`Gateway ${path} did not respond`)), TIME_LIMIT);
    connection.on("error", error => { clearTimeout(deadline); req.reject(error); });
    connection.end(data);
    return req.promise;
  }

  async call(path: string, options: Parameters<FixtureProcess["open"]>[1] = {}):
    Promise<{ readonly http: number | undefined } & Record<string, unknown>> {
    const response = await this.open(path, options);
    const body = await within((async () => {
      response.setEncoding("utf8");
      let text = "";
      for await (const chunk of response) text += String(chunk);
      return text;
    })(), `Gateway ${path} body`);
    return { http: response.statusCode, ...record(JSON.parse(body)) };
  }

  async attach(client = HEAD, resumed = false) {
    assert.deepEqual(await this.call("/attach", { client, body: { pid: headPid, resumed } }), { http: 200, ok: true });
    const response = await this.open("/events", { client });
    assert.equal(response.statusCode, 200);
    assert.match(response.headers["content-type"] ?? "", /^application\/x-ndjson\b/);
    this.streams.add(response);
    response.once("close", () => this.streams.delete(response));
    const frames = new Lines(response);
    return {
      next: () => frames.next("Gateway event"),
      receipt: (sequence: unknown, outcome = "accepted") => this.call("/receipt", { client, body: { sequence, outcome } }),
      async end() {
        // A lost attachment is observable separately from an explicit detach.
        // Buffering any extra NDJSON frame fails this assertion.
        try {
          const frame = await frames.next("Gateway stream closure");
          assert.fail(`expected stream EOF, received ${JSON.stringify(frame)}`);
        } catch (error) {
          if (!(error instanceof Error) || (error.message !== "Gateway stream closure: unexpected EOF" &&
            !("code" in error && error.code === "ECONNRESET"))) throw error;
        }
      },
    };
  }

  async status() {
    const response = await this.call("/status", { unauthenticated: true });
    assert.equal(response.http, 200);
    assert.equal(response.ok, true);
    const value = record(response.value);
    const gateway = record(value.gateway);
    const head = record(value.head);
    assert.equal(gateway.pid, this.child.pid);
    assert.equal(head.pid, headPid);
    const { ready, admissionReady, pendingEvents, unresolvedEvents, stopping } = gateway;
    return { gateway: { ready, admissionReady, pendingEvents, unresolvedEvents, stopping },
      head: { ready: head.ready, connected: head.connected } };
  }

  async close(): Promise<void> {
    await this.write({ command: "stop" });
    this.child.stdin.end();
    assert.equal(await within(this.child.exited, "Gateway graceful exit"), 0);
  }

  async crash(): Promise<void> {
    this.child.kill("SIGKILL");
    await within(this.child.exited, "Gateway crash");
    assert.equal(this.child.signalCode, "SIGKILL");
  }

  async dispose(): Promise<void> {
    for (const response of this.streams) response.destroy();
    this.controls.close();
    if (this.child.exitCode === null && this.child.signalCode === null) this.child.kill("SIGKILL");
    await within(this.child.exited, "Gateway fixture cleanup");
  }
}
