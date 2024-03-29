import { Granularity } from "../types.js";

export class BaseDialect {
  withGranularity(granularity: Granularity, sql: string) {
    switch (granularity) {
      case "day":
        return `EXTRACT(DAY FROM ${sql})`;
      case "week":
        return `EXTRACT(WEEK FROM ${sql})`;
      case "month":
        return `EXTRACT(MONTH FROM ${sql})`;
      case "quarter":
        return `EXTRACT(QUARTER FROM ${sql})`;
      case "year":
        return `EXTRACT(YEAR FROM ${sql})`;
      case "hour":
        return `EXTRACT(HOUR FROM ${sql})`;
      case "minute":
        return `EXTRACT(MINUTE FROM ${sql})`;
      case "second":
        return `EXTRACT(SECOND FROM ${sql})`;
      default:
        // biome-ignore lint/correctness/noSwitchDeclarations: Exhaustiveness check
        const _exhaustiveCheck: never = granularity;
        throw new Error(`Unrecognized granularity: ${granularity}`);
    }
  }
  asIdentifier(value: string) {
    if (value === "*") return value;
    return `"${value}"`;
  }
}
