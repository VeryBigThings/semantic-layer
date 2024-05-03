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

export type OrderDirection = "asc" | "desc";
export type Order<DN extends string = string, MN extends string = string> = {
  member: DN | MN;
  direction: OrderDirection;
};

export type WithInQueryFilter<
  F extends AnyQueryFilter,
  Q extends AnyInputQuery,
> = [Extract<F, { operator: "inQuery" }>] extends [never]
  ? F
  :
      | Exclude<F, { operator: "inQuery" }>
      | {
          operator: "inQuery";
          member: InputQueryDN<Q> | InputQueryMN<Q>;
          value: Q;
        };

export type WithNotInQueryFilter<
  F extends AnyQueryFilter,
  Q extends AnyInputQuery,
> = [Extract<F, { operator: "notInQuery" }>] extends [never]
  ? F
  :
      | Exclude<F, { operator: "notInQuery" }>
      | {
          operator: "notInQuery";
          member: InputQueryDN<Q> | InputQueryMN<Q>;
          value: Q;
        };

export type Query = {
  dimensions?: string[];
  metrics?: string[];
  order?: Order[];
  filters?: AnyQueryFilter;
  limit?: number;
  offset?: number;
};

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

export const GranularityIndex = {
  time: {
    description: "Time of underlying field. Example output: 00:00:00",
    type: "time",
  },
  date: {
    description: "Date of underlying field. Example output: 2021-01-01",
    type: "date",
  },
  year: {
    description: "Year of underlying field. Example output: 2021",
    type: "number",
  },
  quarter: {
    description: "Quarter of underlying field. Example output: 2021-Q1",
    type: "string",
  },
  quarter_of_year: {
    description: "Quarter of year of underlying field. Example output: 1",
    type: "number",
  },
  month: {
    description: "Month of underlying field. Example output: 2021-01",
    type: "string",
  },
  month_num: {
    description: "Month number of underlying field. Example output: 1",
    type: "number",
  },
  week: {
    description: "Week of underlying field. Example output: 2021-W01",
    type: "string",
  },
  week_num: {
    description: "Week number of underlying field. Example output: 1",
    type: "number",
  },
  day_of_month: {
    description: "Day of month of underlying field. Example output: 1",
    type: "number",
  },
  hour: {
    description:
      "Datetime of the underlying field truncated to the hour. Example output: 2021-01-01 00",
    type: "string",
  },
  hour_of_day: {
    description: "Hour of underlying field. Example output: 00",
    type: "string",
  },
  minute: {
    description:
      "Datetime of the underlying field truncated to the minute. Example output: 2021-01-01 00:00",
    type: "string",
  },
} as const satisfies Record<string, { description: string; type: MemberType }>;

export type GranularityIndex = typeof GranularityIndex;

export type GranularityToMemberType = {
  [K in keyof GranularityIndex]: GranularityIndex[K]["type"];
};

function granularities<T extends (keyof GranularityIndex)[]>(
  ...granularities: T
): T[number][] {
  return granularities;
}

export const GranularityByDimensionType = {
  time: granularities("hour", "hour_of_day", "minute"),
  date: granularities(
    "year",
    "quarter",
    "quarter_of_year",
    "month",
    "month_num",
    "week",
    "week_num",
    "day_of_month",
  ),
  datetime: granularities(
    "time",
    "date",
    "year",
    "quarter",
    "quarter_of_year",
    "month",
    "month_num",
    "week",
    "week_num",
    "day_of_month",
    "hour",
    "hour_of_day",
    "minute",
  ),
} as const;

export type GranularityByDimensionType = typeof GranularityByDimensionType;
export type Granularity = keyof typeof GranularityIndex;

export type DimensionWithGranularity<
  D extends string,
  T extends keyof GranularityByDimensionType,
  GT extends keyof GranularityIndex = GranularityByDimensionType[T][number],
> = {
  [K in GT as `${D}.${K}`]: GranularityToMemberType[K];
};

export type MemberType =
  | "string"
  | "number"
  | "date"
  | "datetime"
  | "time"
  | "boolean";
export type MemberFormat = "percentage" | "currency";

export type MemberNameToType = { [k in never]: MemberType };

export type QueryReturnType<
  M extends MemberNameToType,
  N extends keyof M,
  S = Pick<M, N>,
> = {
  [K in keyof S as Replace<
    string & K,
    ".",
    "___",
    { all: true }
  >]: S[K] extends "string"
    ? string
    : S[K] extends "number"
      ? number
      : S[K] extends "date"
        ? Date
        : S[K] extends "time"
          ? string
          : S[K] extends "datetime"
            ? Date
            : S[K] extends "boolean"
              ? boolean
              : never;
};

export type ProcessTOverridesNames<T extends Record<string, unknown>> = {
  [K in keyof T as Replace<string & K, ".", "___", { all: true }>]: T[K];
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

export type QueryMemberName<T extends unknown[]> = T[number] & string;
export type QueryMetricName<T> = Extract<
  T extends unknown[] ? T[number] : never,
  string
>;

export type QueryAdHocMetricType<N extends string> = {
  [K in N as Replace<K, ".", "___", { all: true }>]: unknown;
};

export type AvailableDialects = "postgresql" | "ansi" | "databricks";

export type IntrospectionResult = Record<
  string,
  {
    memberType: "dimension" | "metric";
    path: string;
    format?: MemberFormat | undefined;
    type: MemberType | "unknown";
    description?: string | undefined;
    isPrimaryKey: boolean;
    isGranularity: boolean;
  }
>;

export type InputQuery<DN extends string, MN extends string, F = never> = {
  members: (DN | MN)[];
  order?: Order<DN, MN>[];
  filters?: WithNotInQueryFilter<
    WithInQueryFilter<QueryFilter<F>, InputQuery<DN, MN, F>>,
    InputQuery<DN, MN, F>
  >[];
  limit?: number;
  offset?: number;
};

// biome-ignore lint/suspicious/noExplicitAny: Any used for inference
export type InputQueryDN<Q> = Q extends InputQuery<infer DN, any, any>
  ? DN
  : never;
// biome-ignore lint/suspicious/noExplicitAny: Any used for inference
export type InputQueryMN<Q> = Q extends InputQuery<any, infer MN, any>
  ? MN
  : never;

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type AnyInputQuery = InputQuery<string, string, any>;
