import {
  AndConnective,
  AnyQueryFilter,
  FilterType,
  OrConnective,
  SqlWithBindings,
} from "../types.js";
import {
  afterDate as filterAfterDate,
  beforeDate as filterBeforeDate,
} from "./filter-builder/date-filter-builder.js";
import {
  inDateRange as filterInDateRange,
  notInDateRange as filterNotInDateRange,
} from "./filter-builder/date-range-filter-builder.js";
import { equals as filterEquals, filterIn } from "./filter-builder/equals.js";
import {
  AnyFilterFragmentBuilder,
  GetFilterFragmentBuilderPayload,
} from "./filter-builder/filter-fragment-builder.js";
import {
  contains as filterContains,
  endsWith as filterEndsWith,
  notContains as filterNotContains,
  notEndsWith as filterNotEndsWith,
  notStartsWith as filterNotStartsWith,
  startsWith as filterStartsWith,
} from "./filter-builder/ilike-filter-builder.js";
import {
  notEquals as filterNotEquals,
  notIn as filterNotIn,
} from "./filter-builder/not-equals.js";
import {
  notSet as filterSet,
  set as filterNotSet,
} from "./filter-builder/null-check-filter-builder.js";
import {
  gt as filterGt,
  gte as filterGte,
  lt as filterLt,
  lte as filterLte,
} from "./filter-builder/number-comparison-filter-builder.js";
import {
  inQuery as filterInQuery,
  notInQuery as filterNotInQuery,
} from "./filter-builder/query-filter-builder.js";

import { AnyQueryBuilder } from "../query-builder.js";
import { sqlAsSqlWithBindings } from "./util.js";

export class FilterBuilder {
  private readonly referencedModels: Set<string>;

  constructor(
    private readonly filterFragmentBuilders: Record<
      string,
      AnyFilterFragmentBuilder
    >,
    public readonly queryBuilder: AnyQueryBuilder,
    private readonly filterType: FilterType,
    referencedModels: string[],
    private readonly metricPrefixes?: Record<string, string>,
  ) {
    this.referencedModels = new Set(referencedModels);
  }
  getMemberSql(
    memberName: string,
    context: unknown,
  ): SqlWithBindings | undefined {
    const member = this.queryBuilder.repository.getMember(memberName);
    if (this.referencedModels.has(member.model.name)) {
      if (this.filterType === "dimension" && member.isDimension()) {
        return member.getSql(this.queryBuilder.dialect, context);
      }
      if (this.filterType === "metric" && member.isMetric()) {
        const prefix = this.metricPrefixes?.[member.model.name];
        const sql = member.getAlias(this.queryBuilder.dialect);
        return sqlAsSqlWithBindings(
          prefix
            ? `${this.queryBuilder.dialect.asIdentifier(prefix)}.${sql}`
            : sql,
        );
      }
    }
  }

  buildOr(filter: OrConnective, context: unknown): SqlWithBindings | undefined {
    return this.buildFilters(filter.filters, "or", context);
  }
  buildAnd(
    filter: AndConnective,
    context: unknown,
  ): SqlWithBindings | undefined {
    return this.buildFilters(filter.filters, "and", context);
  }
  buildFilter(
    filter: AnyQueryFilter,
    context: unknown,
  ): SqlWithBindings | undefined {
    if (filter.operator === "and") {
      return this.buildAnd(filter, context);
    }
    if (filter.operator === "or") {
      return this.buildOr(filter, context);
    }
    const memberSql = this.getMemberSql(filter.member, context);
    if (memberSql) {
      const builder = this.filterFragmentBuilders[filter.operator];
      if (builder) {
        return builder.build(this, context, memberSql, filter);
      }
      throw new Error(`Unknown filter operator: ${filter.operator}`);
    }
  }
  buildFilters(
    filters: AnyQueryFilter[],
    connective: "and" | "or",
    context: unknown,
  ): SqlWithBindings | undefined {
    const result = filters.reduce<{ sqls: string[]; bindings: unknown[] }>(
      (acc, filter) => {
        const result = this.buildFilter(filter, context);
        if (result) {
          acc.sqls.push(result.sql);
          acc.bindings.push(...result.bindings);
        }
        return acc;
      },
      { sqls: [], bindings: [] },
    );

    if (result.sqls.length === 0) {
      return;
    }

    if (result.sqls.length === 1) {
      return {
        sql: result.sqls[0]!,
        bindings: result.bindings,
      };
    }

    return {
      sql: `(${result.sqls.join(` ${connective} `)})`,
      bindings: result.bindings,
    };
  }
}

export class FilterFragmentBuilderRegistry<T = never> {
  private readonly filterFragmentBuilders: Record<
    string,
    AnyFilterFragmentBuilder
  > = {};

  register<F extends AnyFilterFragmentBuilder>(
    builder: F,
  ): FilterFragmentBuilderRegistry<T | GetFilterFragmentBuilderPayload<F>> {
    this.filterFragmentBuilders[builder.operator] = builder;
    return this;
  }
  getFilterFragmentBuilders() {
    return Object.values(this.filterFragmentBuilders);
  }
  getFilterBuilder(
    queryBuilder: AnyQueryBuilder,
    filterType: FilterType,
    referencedModels: string[],
    metricPrefixes?: Record<string, string>,
  ): FilterBuilder {
    return new FilterBuilder(
      this.filterFragmentBuilders,
      queryBuilder,
      filterType,
      referencedModels,
      metricPrefixes,
    );
  }
}

export type AnyFilterFragmentBuilderRegistry =
  FilterFragmentBuilderRegistry<any>;
export type GetFilterFragmentBuilderRegistryPayload<T> =
  T extends FilterFragmentBuilderRegistry<infer P> ? P : never;

export function defaultFilterFragmentBuilderRegistry() {
  const registry = new FilterFragmentBuilderRegistry();
  return registry
    .register(filterEquals)
    .register(filterIn)
    .register(filterNotEquals)
    .register(filterNotIn)
    .register(filterSet)
    .register(filterNotSet)
    .register(filterContains)
    .register(filterNotContains)
    .register(filterStartsWith)
    .register(filterNotStartsWith)
    .register(filterEndsWith)
    .register(filterNotEndsWith)
    .register(filterGt)
    .register(filterGte)
    .register(filterLt)
    .register(filterLte)
    .register(filterInDateRange)
    .register(filterNotInDateRange)
    .register(filterBeforeDate)
    .register(filterAfterDate)
    .register(filterInQuery)
    .register(filterNotInQuery);
}
