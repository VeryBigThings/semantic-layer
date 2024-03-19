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

export interface TableQuery {
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
  referencedTables: {
    all: string[];
    dimensions: string[];
    metrics: string[];
  };
  tableQueries: Record<string, TableQuery>;
  metricTable: string | null;
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
