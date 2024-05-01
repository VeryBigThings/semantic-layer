import { AnsiDialect } from "./ansi.js";

export class DatabricksDialect extends AnsiDialect {
  asIdentifier(value: string) {
    if (value === "*") return value;
    return `\`${value}\``;
  }
}
