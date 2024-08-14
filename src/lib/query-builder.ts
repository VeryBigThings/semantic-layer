import {
  AnyInputQuery,
  AnyMemberFormat,
  FilterType,
  HierarchyConfig,
  InputQuery,
  IntrospectionResult,
  MemberNameToType,
  Query,
  QueryMemberName,
  QueryReturnType,
  SqlQueryResult,
} from "./types.js";

import invariant from "tiny-invariant";
import { Simplify } from "type-fest";
import { AnyBaseDialect } from "./dialect/base.js";
import { SqlQuery } from "./dialect/sql-query-builder/to-sql.js";
import { HierarchyElementConfig } from "./hierarchy.js";
import { buildQuery } from "./query-builder/build-query.js";
import { FilterBuilder } from "./query-builder/filter-builder.js";
import { findOptimalJoinGraph } from "./query-builder/optimal-join-graph.js";
import { processQueryAndExpandToSegments } from "./query-builder/process-query-and-expand-to-segments.js";
import { QuerySchema, buildQuerySchema } from "./query-schema.js";
import type { AnyRepository } from "./repository.js";

function isValidGranularityConfigElements(
  elements: HierarchyElementConfig[],
): elements is [HierarchyElementConfig, ...HierarchyElementConfig[]] {
  return elements.length > 0;
}

function transformInputQueryToQuery(
  queryBuilder: AnyQueryBuilder,
  parsedQuery: AnyInputQuery,
) {
  const { members, ...restQuery } = parsedQuery;
  const dimensionsAndMetrics = members.reduce<{
    dimensions: string[];
    metrics: string[];
  }>(
    (acc, memberName) => {
      const member = queryBuilder.repository.getMember(memberName);
      if (member.isDimension()) {
        acc.dimensions.push(memberName);
      } else {
        acc.metrics.push(memberName);
      }

      return acc;
    },
    { dimensions: [], metrics: [] },
  );

  return {
    ...dimensionsAndMetrics,
    ...restQuery,
  } as Query;
}
export class QueryBuilder<
  C,
  D extends MemberNameToType,
  M extends MemberNameToType,
  F,
  P,
  G,
> {
  public readonly querySchema: QuerySchema;
  public readonly hierarchies: HierarchyConfig[];
  public readonly hierarchiesByName: Record<string, HierarchyConfig>;
  constructor(
    public readonly repository: AnyRepository,
    public readonly dialect: AnyBaseDialect,
  ) {
    this.querySchema = buildQuerySchema(this);
    this.hierarchies = this.getGranularityConfigs(repository);
    this.hierarchiesByName = this.hierarchies.reduce<
      Record<string, HierarchyConfig>
    >((acc, hierarchy) => {
      acc[hierarchy.name] = hierarchy;
      return acc;
    }, {});
  }

  private getGranularityConfigs(repository: AnyRepository) {
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

  getHierarchy<G1 extends G>(hierarchyName: G1 & string) {
    return this.unsafeGetHierarchy(hierarchyName);
  }

  unsafeBuildGenericQueryWithoutSchemaParse(
    parsedQuery: AnyInputQuery,
    context: unknown,
  ): SqlQuery {
    const transformedQuery = transformInputQueryToQuery(this, parsedQuery);
    const { query, referencedModels, segments } =
      processQueryAndExpandToSegments(this.repository, transformedQuery);

    const joinGraph = findOptimalJoinGraph(
      this.repository.graph,
      referencedModels.all,
    );

    const sqlQuery = buildQuery(
      this,
      context,
      query,
      referencedModels,
      joinGraph,
      segments,
    );

    return sqlQuery.toSQL();
  }

  unsafeBuildQuery(payload: unknown, context: unknown) {
    const parsedQuery: AnyInputQuery = this.querySchema.parse(payload);
    const { sql, bindings } = this.unsafeBuildGenericQueryWithoutSchemaParse(
      parsedQuery,
      context,
    ).toNative();
    return {
      sql,
      bindings: bindings as P,
    };
  }

  buildQuery<const Q extends { members: string[] }>(
    query: Q &
      InputQuery<
        string & keyof D,
        string & keyof M,
        F & { member: string & (keyof D | keyof M) }
      >,
    ...rest: C extends undefined ? [] : [C]
  ) {
    const [context] = rest;
    const { sql, bindings } = this.unsafeBuildQuery(query, context);

    const result: SqlQueryResult<
      Simplify<
        QueryReturnType<
          D & M,
          QueryMemberName<Q["members"]> & (keyof D | keyof M)
        >
      >,
      P
    > = {
      sql,
      bindings: bindings as P,
    };

    return result;
  }

  getFilterBuilder(
    filterType: FilterType,
    referencedModels: string[],
    metricPrefixes?: Record<string, string>,
  ): FilterBuilder {
    return this.repository
      .getFilterFragmentBuilderRegistry()
      .getFilterBuilder(this, filterType, referencedModels, metricPrefixes);
  }

  introspect(query: AnyInputQuery): IntrospectionResult {
    return query.members.reduce<IntrospectionResult>((acc, memberName) => {
      const member = this.repository.getMember(memberName);
      const isDimension = member.isDimension();

      acc[memberName.replaceAll(".", "___")] = {
        memberType: isDimension ? "dimension" : "metric",
        path: member.getPath(),
        format: member.getFormat() as AnyMemberFormat,
        type: member.getType(),
        description: member.getDescription(),
        isPrimaryKey: isDimension ? member.isPrimaryKey() : false,
        isGranularity: isDimension ? member.isGranularity() : false,
      };

      return acc;
    }, {});
  }
}

export type QueryBuilderQuery<Q> = Q extends QueryBuilder<
  any,
  infer D,
  infer M,
  infer F,
  any,
  any
>
  ? InputQuery<
      string & keyof D,
      string & keyof M,
      F & { member: string & (keyof D | keyof M) }
    >
  : never;

export type AnyQueryBuilder = QueryBuilder<any, any, any, any, any, any>;
