import {
  AnyQuery,
  AnyQueryFilter,
  MemberNameToType,
  Query,
  QueryMemberName,
  QueryReturnType,
  SqlQueryResult,
} from "./types.js";

import knex from "knex";
import { ZodSchema, z } from "zod";

import { Simplify } from "type-fest";
import { BaseDialect } from "./dialect/base.js";
import { buildQuery } from "./query-builder/build-query.js";
import { expandQueryToSegments } from "./query-builder/expand-query.js";
import { findOptimalJoinGraph } from "./query-builder/optimal-join-graph.js";
import type { AnyRepository } from "./repository.js";

function isNonEmptyArray<T>(arr: T[]): arr is [T, ...T[]] {
  return arr.length > 0;
}

function getMemberNamesSchema(memberPaths: string[]) {
  if (isNonEmptyArray(memberPaths)) {
    const [first, ...rest] = memberPaths;
    return z.array(z.enum([first, ...rest])).optional();
  }
  return z.array(z.never()).optional();
}

export class QueryBuilder<
  D extends MemberNameToType,
  M extends MemberNameToType,
  F,
> {
  public readonly querySchema: ZodSchema;
  constructor(
    private readonly repository: AnyRepository,
    private readonly Dialect: typeof BaseDialect,
    private readonly client: knex.Knex,
  ) {
    this.querySchema = this.buildQuerySchema();
  }

  private buildQuerySchema() {
    const dimensionPaths = this.repository
      .getDimensions()
      .map((d) => d.getPath());
    const metricPaths = this.repository.getMetrics().map((m) => m.getPath());
    const memberPaths = [...dimensionPaths, ...metricPaths];

    const registeredFilterFragmentBuildersSchemas = this.repository
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
        dimensions: getMemberNamesSchema(dimensionPaths),
        metrics: getMemberNamesSchema(metricPaths),
        filters: filters.optional(),
        limit: z.number().optional(),
        offset: z.number().optional(),
        order: z.record(z.enum(["asc", "desc"])).optional(),
      })
      .refine(
        (arg) => (arg.dimensions?.length ?? 0) + (arg.metrics?.length ?? 0) > 0,
        "At least one dimension or metric must be selected",
      );

    return schema;
  }

  unsafeBuildQuery(payload: unknown) {
    const query: AnyQuery = this.querySchema.parse(payload);

    const { referencedModels, segments } = expandQueryToSegments(
      this.repository,
      query,
    );

    const joinGraph = findOptimalJoinGraph(
      this.repository.graph,
      referencedModels.all,
    );

    const sqlQuery = buildQuery(
      this.client,
      this.repository,
      this.Dialect,
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

  buildQuery<const Q extends { dimensions?: string[]; metrics?: string[] }>(
    query: Q &
      Query<
        string & keyof D,
        string & keyof M,
        F & { member: string & (keyof D | keyof M) }
      >,
  ) {
    const { sql, bindings } = this.unsafeBuildQuery(query);

    const result: SqlQueryResult<
      Simplify<
        QueryReturnType<
          D & M,
          | (QueryMemberName<Q["dimensions"]> & keyof D)
          | (QueryMemberName<Q["metrics"]> & keyof M)
        >
      >
    > = {
      sql,
      bindings: bindings as unknown[],
    };

    return result;
  }
}