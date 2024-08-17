import { TemporalGranularity } from "../types.js";
import { exhaustiveCheck } from "../util.js";
import { AnsiDialect } from "./ansi.js";

export class DatabricksDialect extends AnsiDialect {
  withGranularity(granularity: TemporalGranularity, sql: string) {
    switch (granularity) {
      case "time":
        return `CAST(${sql} AS TIME)`;
      case "date":
        return `CAST(${sql} AS DATE)`;
      case "year":
        return `EXTRACT(YEAR FROM ${sql})`;
      case "quarter":
        return `EXTRACT(YEAR FROM ${sql}) || '-' || 'Q' || EXTRACT(QUARTER FROM ${sql})`;
      case "quarter_of_year":
        return `EXTRACT(QUARTER FROM ${sql})`;
      case "month":
        return `EXTRACT (YEAR FROM ${sql}) || '-' || LPAD(CAST(EXTRACT(MONTH FROM ${sql}) AS STRING), 2, '0')`;
      case "month_num":
        return `EXTRACT(MONTH FROM ${sql})`;
      case "week":
        return `EXTRACT (YEAR FROM ${sql}) || '-' || 'W' || LPAD(CAST(EXTRACT(WEEK FROM ${sql}) AS STRING), 2, '0')`;
      case "week_num":
        return `EXTRACT(WEEK FROM ${sql})`;
      case "day_of_month":
        return `EXTRACT(DAY FROM ${sql})`;
      case "hour":
        return `CAST(${sql} AS DATE) || ' ' || LPAD(CAST(EXTRACT(HOUR FROM ${sql}) AS STRING), 2, '0')`;
      case "hour_of_day":
        return `EXTRACT(HOUR FROM ${sql})`;
      case "minute":
        return `CAST(${sql} AS DATE) || ' ' || LPAD(CAST(EXTRACT(HOUR FROM ${sql}) AS STRING), 2, '0') || ':' || LPAD(CAST(EXTRACT(MINUTE FROM ${sql}) AS STRING), 2, '0')`;

      default:
        return exhaustiveCheck(
          granularity,
          `Unrecognized granularity: ${granularity}`,
        );
    }
  }
  asIdentifier(value: string) {
    if (value === "*") return value;
    return `\`${value}\``;
  }
}
