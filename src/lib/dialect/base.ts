import { From, SqlFragment, SqlQueryBuilder } from "./sql-query-builder.js";

import { Granularity } from "../types.js";

export abstract class BaseDialect {
  abstract withGranularity(granularity: Granularity, sql: string): string;

  abstract asIdentifier(value: string): string;

  abstract ilike(
    startsWith: boolean,
    endsWith: boolean,
    negation: boolean,
    memberSql: string,
  ): string;

  abstract from(from: From): SqlQueryBuilder;

  abstract fragment(string: string, bindings?: unknown[]): SqlFragment;

  abstract sqlToNative(sql: string): string;
}
