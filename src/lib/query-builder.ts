import {
  AnyQuery,
  AnyQueryFilter,
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
import { z } from "zod";

import { Simplify } from "type-fest";
import { BaseDialect } from "./dialect/base.js";
import { buildQuery } from "./query-builder/build-query.js";
import { findOptimalJoinGraph } from "./query-builder/optimal-join-graph.js";
import { processQueryAndExpandToSegments } from "./query-builder/process-query-and-expand-to-segments.js";
import type { AnyRepository } from "./repository.js";

function getDimensionNamesSchema(dimensionPaths: string[]) {
  return z
    .array(
      z
        .string()
        .refine((arg) => dimensionPaths.includes(arg))
        .describe("Dimension name"),
    )
    .optional();
}

function getMetricNamesSchema(metricPaths: string[], dimensionPaths: string[]) {
  const adHocMetricSchema = z.object({
    aggregateWith: z.enum(["sum", "count", "min", "max", "avg"]),
    dimension: z
      .string()
      .refine((arg) => dimensionPaths.includes(arg))
      .describe("Dimension name"),
  });

  return z
    .array(
      z
        .string()
        .refine((arg) => metricPaths.includes(arg))
        .describe("Metric name")
        .or(adHocMetricSchema),
    )
    .optional();
}

export function buildQuerySchema(repository: AnyRepository) {
  const dimensionPaths = repository.getDimensions().map((d) => d.getPath());
  const metricPaths = repository.getMetrics().map((m) => m.getPath());
  const memberPaths = [...dimensionPaths, ...metricPaths];

  const registeredFilterFragmentBuildersSchemas = repository
    .getFilterFragmentBuilderRegistry()
    .getFilterFragmentBuilders()
    .map((builder) => builder.fragmentBuilderSchema);

  const filters: z.ZodType<AnyQueryFilter[]> = z.array(
    z.union([
      z.object({
        operator: z.literal("and"),
        filters: z.lazy(() => filters),
      }),
      z.object({
        operator: z.literal("or"),
        filters: z.lazy(() => filters),
      }),
      ...registeredFilterFragmentBuildersSchemas.map((schema) =>
        schema.refine((arg) => memberPaths.includes(arg.member), {
          path: ["member"],
          message: "Member not found",
        }),
      ),
    ]),
  );

  const schema = z
    .object({
      dimensions: getDimensionNamesSchema(dimensionPaths),
      metrics: getMetricNamesSchema(metricPaths, dimensionPaths),
      filters: filters.optional(),
      limit: z.number().optional(),
      offset: z.number().optional(),
      order: z.record(z.string(), z.enum(["asc", "desc"])).optional(),
    })
    .refine(
      (arg) => (arg.dimensions?.length ?? 0) + (arg.metrics?.length ?? 0) > 0,
      "At least one dimension or metric must be selected",
    );

  return schema;
}

export class QueryBuilder<
  C,
  D extends MemberNameToType,
  M extends MemberNameToType,
  F,
> {
  public readonly querySchema: ReturnType<typeof buildQuerySchema>;
  constructor(
    private readonly repository: AnyRepository,
    private readonly dialect: BaseDialect,
    private readonly client: knex.Knex,
  ) {
    this.querySchema = buildQuerySchema(repository);
  }

  unsafeBuildQuery(payload: unknown, context: unknown) {
    const parsedQuery: AnyQuery = this.querySchema.parse(payload);

    const { query, referencedModels, segments } =
      processQueryAndExpandToSegments(this.repository, parsedQuery);

    const joinGraph = findOptimalJoinGraph(
      this.repository.graph,
      referencedModels.all,
    );

    const sqlQuery = buildQuery(
      this.client,
      this.repository,
      this.dialect,
      context,
      query,
      referencedModels,
      joinGraph,
      segments,
    );

    const { sql, bindings } = sqlQuery.toSQL().toNative();

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
          acc[
            `${dimensionName.replaceAll(".", "___")}___adhoc_${aggregateWith}`
          ] = {
            memberType: "metric",
            path: `${member.getPath()}.adhoc_${aggregateWith}`,
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
