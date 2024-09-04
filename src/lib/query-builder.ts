import {
  AnyInputQuery,
  AnyMemberFormat,
  HierarchyConfig,
  InputQuery,
  IntrospectionResult,
  MemberNameToType,
  QueryMemberName,
  QueryReturnType,
  SqlQueryResult,
} from "./types.js";

import invariant from "tiny-invariant";
import { Simplify } from "type-fest";
import { AnyBaseDialect } from "./dialect/base.js";
import { pathToAlias } from "./helpers.js";
import { HierarchyElementConfig } from "./hierarchy.js";
import { buildQuery } from "./query-builder/build-query.js";
import { FilterBuilder } from "./query-builder/filter-builder.js";
import { QueryPlan, getQueryPlan } from "./query-builder/query-plan.js";
import { QueryContext } from "./query-builder/query-plan/query-context.js";
import { QuerySchema, buildQuerySchema } from "./query-schema.js";
import type { AnyRepository } from "./repository.js";
import { SqlQuery } from "./sql-builder/to-sql.js";

function isValidGranularityConfigElements(
  elements: HierarchyElementConfig[],
): elements is [HierarchyElementConfig, ...HierarchyElementConfig[]] {
  return elements.length > 0;
}

export class QueryBuilder<
  TContext,
  TDimensions extends MemberNameToType,
  TMetrics extends MemberNameToType,
  TMemberNames extends string,
  TFilters,
  TDialectParamsReturnType,
  THierarchyNames,
> {
  public readonly querySchema: QuerySchema;
  public readonly hierarchies: HierarchyConfig[];
  public readonly hierarchiesByName: Record<string, HierarchyConfig>;
  constructor(
    public readonly repository: AnyRepository,
    public readonly dialect: AnyBaseDialect,
  ) {
    this.querySchema = buildQuerySchema(this);
    this.hierarchies = this.getHierarchyConfigs(repository);
    this.hierarchiesByName = this.hierarchies.reduce<
      Record<string, HierarchyConfig>
    >((acc, hierarchy) => {
      acc[hierarchy.name] = hierarchy;
      return acc;
    }, {});
  }

  private getHierarchyConfigs(repository: AnyRepository) {
    const hierarchies: HierarchyConfig[] = [];
    for (const hierarchy of repository.categoricalHierarchies) {
      const elements = hierarchy.elements.map((element) =>
        element.getConfig(repository),
      );
      invariant(
        isValidGranularityConfigElements(elements),
        "Granularity requires at least one element",
      );
      hierarchies.push({
        name: hierarchy.name,
        type: "categorical",
        elements,
      });
    }
    for (const model of repository.getModels()) {
      for (const hierarchy of model.categoricalHierarchies) {
        const elements = hierarchy.elements.map((element) =>
          element.getConfig(model),
        );
        invariant(
          isValidGranularityConfigElements(elements),
          "Granularity requires at least one element",
        );
        hierarchies.push({
          name: `${model.name}.${hierarchy.name}`,
          type: "categorical",
          elements,
        });
      }
      for (const hierarchy of model.temporalHierarchies) {
        const elements = hierarchy.elements.map((element) =>
          element.getConfig(model),
        );
        invariant(
          isValidGranularityConfigElements(elements),
          "Granularity requires at least one element",
        );
        hierarchies.push({
          name: `${model.name}.${hierarchy.name}`,
          type: "temporal",
          elements,
        });
      }
    }
    for (const hierarchy of repository.temporalHierarchies) {
      const elements = hierarchy.elements.map((element) =>
        element.getConfig(repository),
      );
      invariant(
        isValidGranularityConfigElements(elements),
        "Granularity requires at least one element",
      );
      hierarchies.push({
        name: hierarchy.name,
        type: "temporal",
        elements,
      });
    }
    return hierarchies;
  }

  unsafeGetHierarchy(hierarchyName: string) {
    const hierarchy = this.hierarchiesByName[hierarchyName];
    invariant(hierarchy, `Hierarchy ${hierarchyName} not found`);
    return hierarchy;
  }

  getHierarchy<G1 extends THierarchyNames>(hierarchyName: G1 & string) {
    return this.unsafeGetHierarchy(hierarchyName);
  }

  unsafeBuildGenericQueryWithoutSchemaParse(
    parsedQuery: AnyInputQuery,
    context: unknown,
  ): SqlQuery {
    const queryContext = new QueryContext(
      this.repository,
      this.dialect,
      context,
    );
    const queryPlan = this.getQueryPlan(queryContext, context, parsedQuery);
    const sqlQuery = buildQuery(this, queryContext, context, queryPlan);

    return sqlQuery.toSQL();
  }

  // Return type annotation is needed because otherwise build generates incorrect index.d.ts
  getQueryPlan(
    queryContext: QueryContext,
    context: unknown,
    query: AnyInputQuery,
  ): QueryPlan {
    return getQueryPlan(this, queryContext, context, query);
  }

  unsafeBuildQuery(payload: unknown, context: unknown) {
    const parsedQuery: AnyInputQuery = this.querySchema.parse(payload);
    const { sql, bindings } = this.unsafeBuildGenericQueryWithoutSchemaParse(
      parsedQuery,
      context,
    ).toNative();
    return {
      sql,
      bindings: bindings as TDialectParamsReturnType,
    };
  }

  buildQuery<const Q extends { members: string[] }>(
    query: Q &
      InputQuery<
        string & keyof TDimensions,
        string & keyof TMetrics,
        TFilters & { member: string & TMemberNames }
      >,
    ...rest: TContext extends undefined ? [] : [TContext]
  ) {
    const [context] = rest;
    const { sql, bindings } = this.unsafeBuildQuery(query, context);

    const result: SqlQueryResult<
      Simplify<
        QueryReturnType<
          TDimensions & TMetrics,
          QueryMemberName<Q["members"]> & (keyof TDimensions | keyof TMetrics)
        >
      >,
      TDialectParamsReturnType
    > = {
      sql,
      bindings: bindings as TDialectParamsReturnType,
    };

    return result;
  }

  unsafeBuildCountQuery(payload: unknown, context: unknown) {
    const parsedQuery: AnyInputQuery = this.querySchema.parse(payload);
    const queryContext = new QueryContext(
      this.repository,
      this.dialect,
      context,
    );
    const queryPlan = this.getQueryPlan(queryContext, context, parsedQuery);
    const { sql, bindings } = buildQuery(this, queryContext, context, queryPlan)
      .toCountQuery()
      .toNative();
    return {
      sql: sql,
      bindings: bindings as TDialectParamsReturnType,
    };
  }

  buildCountQuery<const Q extends { members: string[] }>(
    query: Q &
      InputQuery<
        string & keyof TDimensions,
        string & keyof TMetrics,
        TFilters & { member: string & TMemberNames }
      >,
    ...rest: TContext extends undefined ? [] : [TContext]
  ) {
    const [context] = rest;
    const {
      limit: _limit,
      offset: _offset,
      order: _order,
      ...queryWithoutLimitAndOffset
    } = query;

    const { sql, bindings } = this.unsafeBuildCountQuery(
      queryWithoutLimitAndOffset,
      context,
    );

    const result: SqlQueryResult<
      { count: string | number | bigint },
      TDialectParamsReturnType
    > = {
      sql,
      bindings: bindings as TDialectParamsReturnType,
    };

    return result;
  }

  getFilterBuilder(queryContext: QueryContext): FilterBuilder {
    return this.repository
      .getFilterFragmentBuilderRegistry()
      .getFilterBuilder(this, queryContext);
  }

  introspect(query: AnyInputQuery): IntrospectionResult {
    return query.members.reduce<IntrospectionResult>((acc, memberName) => {
      const member = this.repository.getMember(memberName);
      const isDimension = member.isDimension();
      const alias = pathToAlias(memberName);

      acc[alias] = {
        memberType: isDimension ? "dimension" : "metric",
        path: member.getPath(),
        alias,
        format: member.getFormat() as AnyMemberFormat,
        type: member.getType(),
        description: member.getDescription(),
        isPrimaryKey: isDimension ? member.isPrimaryKey() : false,
        isGranularity: isDimension ? member.isGranularity() : false,
        isPrivate: member.isPrivate(),
      };

      return acc;
    }, {});
  }
}

export type QueryBuilderQuery<Q> = Q extends QueryBuilder<
  any,
  infer TDimensions,
  infer TMetrics,
  infer TMemberNames,
  infer TFilters,
  any,
  any
>
  ? InputQuery<
      string & keyof TDimensions,
      string & keyof TMetrics,
      TFilters & {
        member: TMemberNames;
      }
    >
  : never;

export type AnyQueryBuilder = QueryBuilder<any, any, any, any, any, any, any>;
