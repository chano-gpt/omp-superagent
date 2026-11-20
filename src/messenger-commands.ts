import catalog from "./messenger-command-catalog.json" with { type: "json" };

/** Supported owner controls shared by each platform's command picker and the native Gateway. */
export const MESSENGER_COMMANDS: readonly {
  readonly name: string;
  readonly description: string;
  readonly text?: string;
}[] = catalog;
