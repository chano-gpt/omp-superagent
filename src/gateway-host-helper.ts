/** Bounded, on-demand public SDK calls. HEAD ownership and lifecycle stay in Rust. */
export {};

class InvalidHostHelperRequest extends Error {
  readonly name = "InvalidHostHelperRequest";
}

async function run(operation: string | undefined, path: string | undefined): Promise<string | null> {
  if (!path?.startsWith("/") || operation !== "session-id") {
    throw new InvalidHostHelperRequest("Expected session-id and an absolute session path");
  }
  const { readSessionHeaderId } = await import("@oh-my-pi/pi-coding-agent/session/session-loader");
  return await readSessionHeaderId(path) ?? null;
}

try {
  const [, , operation, path] = process.argv;
  process.stdout.write(`GATEWAY_HOST ${JSON.stringify(await run(operation, path))}\n`);
} catch {
  // Do not put private SDK/session diagnostics on the Gateway channel.
  console.error("Gateway Host SDK operation failed");
  process.exitCode = 1;
}
