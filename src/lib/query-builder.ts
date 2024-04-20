import {
  AnyQuery,
  FilterType,
  IntrospectionResult,
  MemberNameToType,
  Query,
  QueryAdHocMetricName,
  QueryAdHocMetricType,
  QueryMemberName,
  QueryMetric,
  QueryMetricName,
  QueryReturnType,
  SqlQueryResult,
} from "./types.js";

import knex from "knex";

import { Simplify } from "type-fest";
import { BaseDialect } from "./dialect/base.js";
import { buildQuery } from "./query-builder/build-query.js";
import { FilterBuilder } from "./query-builder/filter-builder.js";
import { findOptimalJoinGraph } from "./query-builder/optimal-join-graph.js";
import { processQueryAndExpandToSegments } from "./query-builder/process-query-and-expand-to-segments.js";
import { QuerySchema, buildQuerySchema } from "./query-schema.js";
import type { AnyRepository } from "./repository.js";
import { getAdHocAlias, getAdHocPath } from "./util.js";

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
    public readonly client: knex.Knex,
  ) {
    this.querySchema = buildQuerySchema(this);
  }

  unsafeBuildGenericQueryWithoutSchemaParse(
    parsedQuery: AnyQuery,
    context: unknown,
  ) {
    const { query, referencedModels, segments } =
      processQueryAndExpandToSegments(this.repository, parsedQuery);

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
    const parsedQuery: AnyQuery = this.querySchema.parse(payload);
    const { sql, bindings } = this.unsafeBuildGenericQueryWithoutSchemaParse(
      parsedQuery,
      context,
    ).toNative();
    return {
      sql,
      bindings: bindings as unknown[],
    };
  }

  buildQuery<
    const Q extends { dimensions?: string[]; metrics?: QueryMetric[] },
  >(
    query: Q &
      Query<
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
          | (QueryMemberName<Q["dimensions"]> & keyof D)
          | (QueryMetricName<Q["metrics"]> & keyof M)
        > &
          QueryAdHocMetricType<QueryAdHocMetricName<Q["metrics"]>>
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

  introspect(query: AnyQuery): IntrospectionResult {
    const queryDimensions = query.dimensions ?? [];
    const queryMetrics = query.metrics ?? [];

    return [...queryDimensions, ...queryMetrics].reduce<IntrospectionResult>(
      (acc, memberNameOrAdHoc) => {
        if (typeof memberNameOrAdHoc === "string") {
          const member = this.repository.getMember(memberNameOrAdHoc);
          const isDimension = member.isDimension();

          acc[memberNameOrAdHoc.replaceAll(".", "___")] = {
            memberType: isDimension ? "dimension" : "metric",
            path: member.getPath(),
            format: member.getFormat(),
            type: member.getType(),
            description: member.getDescription(),
            isPrimaryKey: isDimension ? member.isPrimaryKey() : false,
            isGranularity: isDimension ? member.isGranularity() : false,
          };
        } else {
          const aggregateWith = memberNameOrAdHoc.aggregateWith;
          const dimensionName = memberNameOrAdHoc.dimension;
          const member = this.repository.getMember(dimensionName);
          acc[getAdHocAlias(dimensionName, aggregateWith)] = {
            memberType: "metric",
            path: getAdHocPath(member.getPath(), aggregateWith),
            format: undefined,
            type: "unknown",
            description: undefined,
            isPrimaryKey: false,
            isGranularity: false,
          };
        }

        return acc;
      },
      {},
    );
  }
}

export type QueryBuilderQuery<Q> = Q extends QueryBuilder<
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  any,
  infer D,
  infer M,
  infer F
>
  ? Query<string & keyof D, string & keyof M, F>
  : never;

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type AnyQueryBuilder = QueryBuilder<any, any, any, any>;
