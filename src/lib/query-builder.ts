import {
  AnyInputQuery,
  FilterType,
  InputQuery,
  IntrospectionResult,
  MemberNameToType,
  Query,
  QueryMemberName,
  QueryReturnType,
  SqlQueryResult,
} from "./types.js";

import { Simplify } from "type-fest";
import { BaseDialect } from "./dialect/base.js";
import { SqlQuery } from "./dialect/sql-query-builder/to-sql.js";
import { buildQuery } from "./query-builder/build-query.js";
import { FilterBuilder } from "./query-builder/filter-builder.js";
import { findOptimalJoinGraph } from "./query-builder/optimal-join-graph.js";
import { processQueryAndExpandToSegments } from "./query-builder/process-query-and-expand-to-segments.js";
import { QuerySchema, buildQuerySchema } from "./query-schema.js";
import type { AnyRepository } from "./repository.js";

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
> {
  public readonly querySchema: QuerySchema;
  constructor(
    public readonly repository: AnyRepository,
    public readonly dialect: BaseDialect,
  ) {
    this.querySchema = buildQuerySchema(this);
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
      bindings: bindings as unknown[],
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
      >
    > = {
      sql,
      bindings: bindings as unknown[],
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
        format: member.getFormat(),
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
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  any,
  infer D,
  infer M,
  infer F
>
  ? InputQuery<string & keyof D, string & keyof M, F>
  : never;

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type AnyQueryBuilder = QueryBuilder<any, any, any, any>;
