import { From, SqlFragment, SqlQueryBuilder } from "./sql-query-builder.js";

import { Granularity } from "../types.js";

export type DialectParamsType = "array" | "object";

export abstract class BaseDialect<P extends DialectParamsType> {
  abstract withGranularity(granularity: Granularity, sql: string): string;

  abstract asIdentifier(value: string): string;

  abstract ilike(
    startsWith: boolean,
    endsWith: boolean,
    negation: boolean,
    memberSql: string,
  ): string;

  from(from: From): SqlQueryBuilder {
    return new SqlQueryBuilder(this, from);
  }

  fragment(string: string, bindings: unknown[] = []) {
    return new SqlFragment(string, bindings);
  }

  abstract sqlToNative(sql: string): string;

  abstract paramsToNative(
    params: unknown[],
  ): P extends "array" ? unknown[] : Record<string, unknown>;

  abstract limitOffset(
    limit: number | undefined | null,
    offset: number,
  ): SqlFragment;
}

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type AnyBaseDialect = BaseDialect<any>;
