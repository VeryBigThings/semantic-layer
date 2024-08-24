import { SqlFragment } from "../sql-builder.js";
import { TemporalGranularity } from "../types.js";
import { exhaustiveCheck } from "../util.js";
import { BaseDialect } from "./base.js";

export class AnsiDialect extends BaseDialect<"array"> {
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
        return `EXTRACT (YEAR FROM ${sql}) || '-' || LPAD(CAST(EXTRACT(MONTH FROM ${sql}) AS CHARACTER VARYING), 2, '0')`;
      case "month_num":
        return `EXTRACT(MONTH FROM ${sql})`;
      case "week":
        return `EXTRACT (YEAR FROM ${sql}) || '-' || 'W' || LPAD(CAST(EXTRACT(WEEK FROM ${sql}) AS CHARACTER VARYING), 2, '0')`;
      case "week_num":
        return `EXTRACT(WEEK FROM ${sql})`;
      case "day_of_month":
        return `EXTRACT(DAY FROM ${sql})`;
      case "hour":
        return `CAST(${sql} AS DATE) || ' ' || LPAD(CAST(EXTRACT(HOUR FROM ${sql}) AS CHARACTER VARYING), 2, '0')`;
      case "hour_of_day":
        return `EXTRACT(HOUR FROM ${sql})`;
      case "minute":
        return `CAST(${sql} AS DATE) || ' ' || LPAD(CAST(EXTRACT(HOUR FROM ${sql}) AS CHARACTER VARYING), 2, '0') || ':' || LPAD(CAST(EXTRACT(MINUTE FROM ${sql}) AS CHARACTER VARYING), 2, '0')`;

      default:
        return exhaustiveCheck(
          granularity,
          `Unrecognized granularity: ${granularity}`,
        );
    }
  }

  asIdentifier(value: string) {
    if (value === "*") {
      return value;
    }
    return `"${value}"`;
  }

  ilike(
    beginWithWildcard: boolean,
    endWithWildcard: boolean,
    negation: boolean,
    memberSql: string,
  ) {
    let like = "?";
    if (beginWithWildcard) {
      like = `'%' || ${like}`;
    }
    if (endWithWildcard) {
      like = `${like} || '%'`;
    }
    if (negation) {
      return `${memberSql} not ilike ${like}`;
    }
    return `${memberSql} ilike ${like}`;
  }

  sqlToNative(sql: string) {
    return sql;
  }

  paramsToNative(params: unknown[]) {
    return params;
  }

  limitOffset(limit: number | undefined | null, offset: number): SqlFragment {
    if (typeof limit === "number" && typeof offset === "number") {
      return new SqlFragment("limit ? offset ?", [limit, offset]);
    }

    return new SqlFragment("offset ?", [offset]);
  }
}
