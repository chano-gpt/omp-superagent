// One shot, under the recorded restart-runner PID. Resolve FILE.env with the
// same private-file checks as `start`; never serialize tokens to argv or pipes.
import { resolve } from "node:path";
import { loadConfig, requireMessengerCredentials } from "./config";
import { loadCredentials } from "./credentials";
import { requireGateway } from "./gateway-native";

const [file, request, originalStateDir, ...extra] = process.argv.slice(2);
if (!file || !request || !originalStateDir || extra.length) throw new Error("Missing restart identity");
const binary = await requireGateway();
let command = "__restart-rejected";
try {
  const config = await loadConfig(file);
  if (config.stateDir !== resolve(originalStateDir)) throw new Error("Restart state directory changed");
  loadCredentials(config);
  requireMessengerCredentials(config);
  command = "__restart-validated";
} catch {
  // The Rust runner conditionally records failure only for its exact PID.
  console.error("Gateway restart configuration or private credentials were rejected");
}
if (!process.execve) throw new Error("Gateway restart requires Bun execve support");
process.execve(binary, [binary, command, "--config", file, "--restart-request", request, "--state-dir", originalStateDir], process.env);
