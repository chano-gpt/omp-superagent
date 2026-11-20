export {};

try {
  const [, , operation, path] = process.argv;
  if (operation !== "session-id" || !path?.startsWith("/")) throw new Error("Expected an absolute session path");
  const { readSessionHeaderId } = await import("@oh-my-pi/pi-coding-agent/session/session-loader");
  const id = await readSessionHeaderId(path);
  process.stdout.write(`GATEWAY_HOST ${JSON.stringify(id ?? null)}\n`);
} catch {
  // Do not put private SDK/session diagnostics on the Gateway channel.
  console.error("Gateway Host SDK operation failed");
  process.exitCode = 1;
}
