// One-shot parser shared with the operator CLI. Only normalized config is
// returned: never load credential files, native OMP, or messenger libraries.
import { loadConfig } from "./config";

const [file, ...extra] = process.argv.slice(2);
if (!file || extra.length) throw new Error("Expected one Gateway config file");
process.stdout.write(JSON.stringify(await loadConfig(file)));
