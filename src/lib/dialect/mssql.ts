import { SqlFragment } from "../sql-builder.js";
import { TemporalGranularity } from "../types.js";
import { exhaustiveCheck } from "../util.js";
import { BaseDialect } from "./base.js";

export class MSSQLDialect extends BaseDialect<"object"> {
  asIdentifier(value: string) {
    if (value === "*") {
      return value;
    }
    return `[${value}]`;
  }
  sqlToNative(sql: string) {
    return this.positionBindings(sql);
  }
  paramsToNative(params: unknown[]) {
    return Object.fromEntries(
      params.map((param, index) => [`param${index + 1}`, param]),
    );
  }
  private positionBindings(sql: string) {
    let questionCount = 0;
    return sql.replace(/(\\*)(\?)/g, (_match, escapes) => {
      if (escapes.length % 2) {
        return "?";
      }
      questionCount++;
      return `@param${questionCount}`;
    });
  }
  withGranularity(granularity: TemporalGranularity, sql: string) {
    switch (granularity) {
      case "time":
        return `FORMAT(${sql}, 'HH:mm:ss')`;
      case "date":
        return `CAST(${sql} AS DATE)`;
      case "year":
        return `YEAR(${sql})`;
      case "quarter":
        return `CAST(YEAR(${sql}) AS CHAR(4)) + '-' + 'Q' + CAST(DATEPART(QUARTER, ${sql}) AS CHAR(1))`;
      case "quarter_of_year":
        return `DATEPART(QUARTER, ${sql})`;
      case "month":
        return `CAST(YEAR(${sql}) AS CHAR(4)) + '-' + FORMAT(MONTH(${sql}), '00')`;
      case "month_num":
        return `MONTH(${sql})`;
      case "week":
        return `CAST(YEAR(${sql}) AS CHAR(4)) + '-' + 'W' + FORMAT(DATEPART(WEEK, ${sql}), '00')`;
      case "week_num":
        return `DATEPART(WEEK, ${sql})`;
      case "day_of_month":
        return `DAY(${sql})`;
      case "hour":
        return `CONVERT(VARCHAR, ${sql}, 23) + ' ' + FORMAT(DATEPART(HOUR, ${sql}), '00')`;
      case "hour_of_day":
        return `DATEPART(HOUR, ${sql})`;
      case "minute":
        return `CONVERT(VARCHAR, ${sql}, 23) + ' ' + FORMAT(DATEPART(HOUR, ${sql}), '00') + ':' + FORMAT(DATEPART(MINUTE, ${sql}), '00')`;

      default:
        return exhaustiveCheck(
          granularity,
          `Unrecognized granularity: ${granularity}`,
        );
    }
  }

  ilike(
    beginWithWildcard: boolean,
    endWithWildcard: boolean,
    negation: boolean,
    memberSql: string,
  ) {
    let like = "LOWER(?)";
    if (beginWithWildcard) {
      like = `'%' + ${like}`;
    }
    if (endWithWildcard) {
      like = `${like} + '%'`;
    }
    if (negation) {
      return `LOWER(${memberSql}) not like ${like}`;
    }
    return `LOWER(${memberSql}) like ${like}`;
  }

  limitOffset(limit: number | undefined | null, offset: number): SqlFragment {
    if (typeof limit === "number" && typeof offset === "number") {
      return new SqlFragment("offset ? rows fetch next ? rows only", [
        offset,
        limit,
      ]);
    }

    return new SqlFragment("offset ?", [offset]);
  }
}
