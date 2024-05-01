import { From, SqlFragment, SqlQueryBuilder } from "./sql-query-builder.js";

import { Granularity } from "../types.js";

export class AnsiDialect {
  withGranularity(granularity: Granularity, sql: string) {
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
        return `EXTRACT (YEAR FROM ${sql}) || '-' || LPAD(EXTRACT(MONTH FROM ${sql})::varchar, 2, '0')`;
      case "month_num":
        return `EXTRACT(MONTH FROM ${sql})`;
      case "week":
        return `EXTRACT (YEAR FROM ${sql}) || '-' || 'W' || LPAD(EXTRACT(WEEK FROM ${sql})::varchar, 2, '0')`;
      case "week_num":
        return `EXTRACT(WEEK FROM ${sql})`;
      case "day_of_month":
        return `EXTRACT(DAY FROM ${sql})`;
      case "hour":
        return `CAST(${sql} AS DATE) || ' ' || LPAD(EXTRACT(HOUR FROM ${sql})::varchar, 2, '0')`;
      case "hour_of_day":
        return `EXTRACT(HOUR FROM ${sql})`;
      case "minute":
        return `CAST(${sql} AS DATE) || ' ' || LPAD(EXTRACT(HOUR FROM ${sql})::varchar, 2, '0') || ':' || LPAD(EXTRACT(MINUTE FROM ${sql})::varchar, 2, '0')`;

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

  ilike(
    startsWith: boolean,
    endsWith: boolean,
    negation: boolean,
    memberSql: string,
  ) {
    let like = "?";
    if (startsWith) {
      like = `'%' || ${like}`;
    }
    if (endsWith) {
      like = `${like} || '%'`;
    }
    if (negation) {
      return `${memberSql} not ilike ${like}`;
    }
    return `${memberSql} ilike ${like}`;
  }

  from(from: From) {
    return new SqlQueryBuilder(this, from);
  }

  fragment(string: string, bindings: unknown[] = []) {
    return new SqlFragment(string, bindings);
  }

  sqlToNative(sql: string) {
    return sql;
  }
}
