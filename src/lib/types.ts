import { Replace, Simplify } from "type-fest";

export interface AndConnective<F = never> {
  operator: "and";
  filters: QueryFilter<F>[];
}

export interface OrConnective<F = never> {
  operator: "or";
  filters: QueryFilter<F>[];
}

export type FilterType = "dimension" | "metric";

export type QueryFilter<F> = F | AndConnective<F> | OrConnective<F>;

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type AnyQueryFilter = QueryFilter<any>;

export type Query<DN extends string, MN extends string, F = never> = {
  dimensions?: DN[];
  metrics?: MN[];
  order?: { [K in DN | MN]?: "asc" | "desc" };
  filters?: QueryFilter<F>[];
  limit?: number;
  offset?: number;
};

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type AnyQuery = Query<string, string, any>;

export interface ModelQuery {
  dimensions: Set<string>;
  metrics: Set<string>;
}

export interface QuerySegmentQuery {
  dimensions: string[];
  metrics: string[];
  filters: AnyQueryFilter[];
}

export interface QuerySegment {
  query: QuerySegmentQuery;
  projectedQuery: QuerySegmentQuery;
  referencedModels: {
    all: string[];
    dimensions: string[];
    metrics: string[];
  };
  modelQueries: Record<string, ModelQuery>;
  metricModel: string | null;
}

export interface SqlWithBindings {
  sql: string;
  bindings: unknown[];
}

export const GranularityByDimensionType = {
  time: ["hour", "minute", "second"],
  date: ["year", "quarter", "month", "week", "day"],
  datetime: [
    "year",
    "quarter",
    "month",
    "week",
    "day",
    "hour",
    "minute",
    "second",
  ],
} as const;

export type GranularityByDimensionType = typeof GranularityByDimensionType;
export type Granularity =
  GranularityByDimensionType[keyof GranularityByDimensionType][number];

export type MemberType = "string" | "number" | "date" | "datetime" | "boolean";
export type MemberFormat = "percentage" | "currency";

export type MemberNameToType = { [k in never]: MemberType };

export type QueryReturnType<
  M extends MemberNameToType,
  N extends keyof M,
  S = Pick<M, N>,
> = {
  [K in keyof S as Replace<string & K, ".", "___">]: S[K] extends "string"
    ? string
    : S[K] extends "number"
      ? number
      : S[K] extends "date"
        ? Date
        : S[K] extends "time"
          ? Date
          : S[K] extends "datetime"
            ? Date
            : S[K] extends "boolean"
              ? boolean
              : never;
};

export type ProcessTOverridesNames<T extends Record<string, unknown>> = {
  [K in keyof T as Replace<string & K, ".", "___">]: T[K];
};

// biome-ignore lint/correctness/noUnusedVariables: We need the RT generic param to be present so we can extract it to infer the return type later
export interface SqlQueryResult<RT extends Record<string, unknown>> {
  sql: string;
  bindings: unknown[];
}

export type MergeInferredSqlQueryResultTypeWithOverrides<
  T extends Record<string, unknown>,
  RT extends Record<string, unknown>,
> = Omit<T, keyof RT> & Pick<RT, string & keyof T>;

export type InferSqlQueryResultType<
  T,
  TOverrides extends Record<string, unknown> = never,
> = T extends SqlQueryResult<infer RT>
  ? [TOverrides] extends [never]
    ? RT
    : Simplify<
        MergeInferredSqlQueryResultTypeWithOverrides<
          RT,
          ProcessTOverridesNames<TOverrides>
        >
      >
  : never;

export type QueryMemberName<T> = T extends string[] ? T[number] : never;

export type AvailableDialects = "postgresql";
