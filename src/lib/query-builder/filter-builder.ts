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
  notSet as filterSet,
  set as filterNotSet,
} from "./filter-builder/null-check-filter-builder.js";
import {
  gt as filterGt,
  gte as filterGte,
  lt as filterLt,
  lte as filterLte,
} from "./filter-builder/number-comparison-filter-builder.js";

import { BaseDialect } from "../dialect/base.js";
import type { AnyRepository } from "../repository.js";
import { equals as filterEquals } from "./filter-builder/equals.js";
import { notEquals as filterNotEquals } from "./filter-builder/not-equals.js";
import { sqlAsSqlWithBindings } from "./util.js";

export class FilterBuilder {
  private readonly referencedModels: Set<string>;

  constructor(
    private readonly filterFragmentBuilders: Record<
      string,
      AnyFilterFragmentBuilder
    >,
    private readonly dialect: BaseDialect,
    private readonly repository: AnyRepository,
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
    const member = this.repository.getMember(memberName);
    if (this.referencedModels.has(member.model.name)) {
      if (this.filterType === "dimension" && member.isDimension()) {
        return member.getSql(this.dialect, context);
      }
      if (this.filterType === "metric" && member.isMetric()) {
        const prefix = this.metricPrefixes?.[member.model.name];
        const sql = member.getAlias(this.dialect);
        return sqlAsSqlWithBindings(
          prefix ? `${this.dialect.asIdentifier(prefix)}.${sql}` : sql,
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
        return builder.build(this, memberSql, filter);
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

  // TODO: capture EXTERNAl type in T of register (register<T = never>(....)) and then
  // propagate these types to the query function. Other option is to capture type of the
  // `filter` argument of the filter fragment builder function
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
    repository: AnyRepository,
    dialect: BaseDialect,
    filterType: FilterType,
    referencedModels: string[],
    metricPrefixes?: Record<string, string>,
  ): FilterBuilder {
    return new FilterBuilder(
      this.filterFragmentBuilders,
      dialect,
      repository,
      filterType,
      referencedModels,
      metricPrefixes,
    );
  }
}

export type AnyFilterFragmentBuilderRegistry =
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  FilterFragmentBuilderRegistry<any>;
export type GetFilterFragmentBuilderRegistryPayload<T> =
  T extends FilterFragmentBuilderRegistry<infer P> ? P : never;

export function defaultFilterFragmentBuilderRegistry() {
  const registry = new FilterFragmentBuilderRegistry();
  return registry
    .register(filterEquals)
    .register(filterNotEquals)
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
    .register(filterAfterDate);
}
