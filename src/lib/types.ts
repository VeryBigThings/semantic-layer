import { Replace, Simplify } from "type-fest";
import { HierarchyElement, HierarchyElementConfig } from "./hierarchy.js";
import {
  AnyQueryBuilder,
  GetQueryBuilderDimensions,
  GetQueryBuilderMetrics,
  QueryBuilderQuery,
} from "./query-builder.js";
import { exhaustiveCheck } from "./util.js";

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

export const TemporalGranularityIndex = {
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
    type: "number",
  },
  minute: {
    description:
      "Datetime of the underlying field truncated to the minute. Example output: 2021-01-01 00:00",
    type: "string",
  },
} as const satisfies Record<string, { description: string; type: MemberType }>;

export type TemporalGranularityIndex = typeof TemporalGranularityIndex;

export type TemporalGranularityToMemberType = {
  [K in keyof TemporalGranularityIndex]: TemporalGranularityIndex[K]["type"];
};

function temporalGranularities<T extends (keyof TemporalGranularityIndex)[]>(
  ...granularities: T
): T[number][] {
  return granularities;
}

export const TemporalGranularityByDimensionType = {
  time: temporalGranularities("hour", "minute"),
  date: temporalGranularities(
    "year",
    "quarter",
    "quarter_of_year",
    "month",
    "month_num",
    "week",
    "week_num",
    "day_of_month",
  ),
  datetime: temporalGranularities(
    "year",
    "quarter",
    "quarter_of_year",
    "month",
    "month_num",
    "week",
    "week_num",
    "date",
    "day_of_month",
    "time",
    "hour",
    "hour_of_day",
    "minute",
  ),
} as const;

export type TemporalGranularityByDimensionType =
  typeof TemporalGranularityByDimensionType;
export type TemporalGranularity = keyof typeof TemporalGranularityIndex;

export type DimensionWithTemporalGranularity<
  D extends string,
  T extends keyof TemporalGranularityByDimensionType,
  GT extends
    keyof TemporalGranularityIndex = TemporalGranularityByDimensionType[T][number],
> = {
  [K in GT as `${D}.${K}`]: TemporalGranularityToMemberType[K];
};

const temporalHierarchyElementsByDimensionType: {
  [K in keyof TemporalGranularityByDimensionType]: TemporalGranularityByDimensionType[K];
} = {
  time: [],
  date: ["year", "quarter", "month", "week"],
  datetime: ["year", "quarter", "month", "week", "date"],
};

export function makeTemporalHierarchyElementsForDimension(
  dimensionName: string,
  dimensionType: "time" | "date" | "datetime",
) {
  switch (dimensionType) {
    case "time":
      return [
        ...temporalHierarchyElementsByDimensionType.time.map((granularity) => {
          const granularityDimensionName = `${dimensionName}.${granularity}`;
          return new HierarchyElement(granularityDimensionName, [
            granularityDimensionName,
          ]);
        }),
        new HierarchyElement(dimensionName, [dimensionName]),
      ];

    case "date": {
      return [
        ...temporalHierarchyElementsByDimensionType.date.map((granularity) => {
          const granularityDimensionName = `${dimensionName}.${granularity}`;
          return new HierarchyElement(granularityDimensionName, [
            granularityDimensionName,
          ]);
        }),
        new HierarchyElement(dimensionName, [dimensionName]),
      ];
    }
    case "datetime": {
      return [
        ...temporalHierarchyElementsByDimensionType.datetime.map(
          (granularity) => {
            const granularityDimensionName = `${dimensionName}.${granularity}`;
            return new HierarchyElement(granularityDimensionName, [
              granularityDimensionName,
            ]);
          },
        ),
        new HierarchyElement(dimensionName, [dimensionName]),
      ];
    }
    default:
      exhaustiveCheck(
        dimensionType,
        `Unrecognized dimension type: ${dimensionType}`,
      );
  }
}

export type MemberType =
  | "string"
  | "number"
  | "boolean"
  | "date"
  | "datetime"
  | "time";

export type MemberTypeToType<MT extends MemberType> = MT extends "number"
  ? number
  : MT extends "date"
    ? Date
    : MT extends "datetime"
      ? Date
      : MT extends "time"
        ? string
        : MT extends "boolean"
          ? boolean
          : string;

export type MemberFormat<MT extends MemberType = MemberType> =
  | "percentage"
  | "currency"
  | {
      [K in MT]: (value: MemberTypeToType<K> | null | undefined) => string;
    }[MT];

export type AnyMemberFormat = MemberFormat<any>;

export type MemberNameToType = { [k in never]: MemberType };

export type MemberProps<
  TSharedAdditionalProps extends object = object,
  TAdditionalPropsForMemberType extends Partial<
    Record<MemberType, object>
  > = Record<MemberType, object>,
> = {
  [K in MemberType]: {
    type: K;
    private?: boolean;
    description?: string;
    format?: MemberFormat<K>;
  } & TSharedAdditionalProps &
    TAdditionalPropsForMemberType[K];
}[MemberType];

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
export interface SqlQueryResult<RT extends Record<string, unknown>, P> {
  sql: string;
  bindings: P;
}

export type MergeInferredSqlQueryResultTypeWithOverrides<
  T extends Record<string, unknown>,
  RT extends Record<string, unknown>,
> = Omit<T, keyof RT> & Pick<RT, string & keyof T>;

export type InferSqlQueryResultType<
  T,
  TOverrides extends Record<string, unknown> = never,
> = T extends SqlQueryResult<infer RT, any>
  ? [TOverrides] extends [never]
    ? RT
    : Simplify<
        MergeInferredSqlQueryResultTypeWithOverrides<
          RT,
          ProcessTOverridesNames<TOverrides>
        >
      >
  : never;

export type InferSqlQueryResultTypeFromQuery<
  TQueryBuilder extends AnyQueryBuilder,
  TQuery extends QueryBuilderQuery<TQueryBuilder>,
  TOverrides extends Record<string, unknown> = never,
> = [TOverrides] extends [never]
  ? QueryReturnType<
      GetQueryBuilderDimensions<TQueryBuilder> &
        GetQueryBuilderMetrics<TQueryBuilder>,
      TQuery["members"][number] &
        (
          | keyof GetQueryBuilderDimensions<TQueryBuilder>
          | keyof GetQueryBuilderMetrics<TQueryBuilder>
        )
    >
  : Simplify<
      MergeInferredSqlQueryResultTypeWithOverrides<
        QueryReturnType<
          GetQueryBuilderDimensions<TQueryBuilder> &
            GetQueryBuilderMetrics<TQueryBuilder>,
          TQuery["members"][number] &
            (
              | keyof GetQueryBuilderDimensions<TQueryBuilder>
              | keyof GetQueryBuilderMetrics<TQueryBuilder>
            )
        >,
        ProcessTOverridesNames<TOverrides>
      >
    >;

export type QueryMemberName<T extends unknown[]> = T[number] & string;
export type QueryMetricName<T> = Extract<
  T extends unknown[] ? T[number] : never,
  string
>;

export type IntrospectionResult = Record<
  string,
  {
    memberType: "dimension" | "metric";
    path: string;
    alias: string;
    format?: AnyMemberFormat | undefined;
    type: MemberType | "unknown";
    description?: string | undefined;
    isPrimaryKey: boolean;
    isGranularity: boolean;
    isPrivate: boolean;
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

export type InputQueryDN<Q> = Q extends InputQuery<infer DN, any, any>
  ? DN
  : never;

export type InputQueryMN<Q> = Q extends InputQuery<any, infer MN, any>
  ? MN
  : never;

export type AnyInputQuery = InputQuery<string, string, any>;

export type HierarchyType = "categorical" | "temporal";
export interface HierarchyConfig {
  name: string;
  type: HierarchyType;
  elements: [HierarchyElementConfig, ...HierarchyElementConfig[]];
}

export type ModelMemberWithoutModelPrefix<
  TModelName extends string,
  TDimensionName extends string,
> = TDimensionName extends `${TModelName}.${infer D}` ? D : never;
