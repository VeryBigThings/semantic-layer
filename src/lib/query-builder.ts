import {
  AnyQuery,
  AnyQueryFilter,
  IntrospectionResult,
  MemberFormat,
  MemberNameToType,
  MemberType,
  Query,
  QueryMemberName,
  QueryReturnType,
  SqlQueryResult,
} from "./types.js";

import knex from "knex";
import { z } from "zod";

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
  return z.array(z.string()).max(0).optional();
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
      dimensions: getMemberNamesSchema(dimensionPaths),
      metrics: getMemberNamesSchema(metricPaths),
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

  buildQuery<const Q extends { dimensions?: string[]; metrics?: string[] }>(
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
          | (QueryMemberName<Q["metrics"]> & keyof M)
        >
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

    return [...queryDimensions, ...queryMetrics].reduce<
      Record<
        string,
        {
          memberType: "dimension" | "metric";
          path: string;
          format?: MemberFormat;
          type: MemberType;
          description?: string;
        }
      >
    >((acc, memberName) => {
      const member = this.repository.getMember(memberName);
      acc[memberName.replaceAll(".", "___")] = {
        memberType: member.isDimension() ? "dimension" : "metric",
        path: member.getPath(),
        format: member.getFormat(),
        type: member.getType(),
        description: member.getDescription(),
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
  ? Query<string & keyof D, string & keyof M, F>
  : never;
