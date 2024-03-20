import {
  MemberNameToType,
  Query,
  QueryMemberName,
  QueryReturnType,
  SqlQueryResult,
} from "./types.js";

import knex from "knex";

import { Simplify } from "type-fest";
import type { AnyRepository } from "./repository.js";
import { BaseDialect } from "./dialect/base.js";
import { expandQueryToSegments } from "./query-builder/expand-query.js";
import { findOptimalJoinGraph } from "./query-builder/optimal-join-graph.js";
import { buildQuery } from "./query-builder/build-query.js";

export class QueryBuilder<
  D extends MemberNameToType,
  M extends MemberNameToType,
  F,
> {
  constructor(
    private readonly repository: AnyRepository,
    private readonly Dialect: typeof BaseDialect,
    private readonly client: knex.Knex,
  ) {}

  build<const Q extends { dimensions?: string[]; metrics?: string[] }>(
    query: Q &
      Query<
        string & keyof D,
        string & keyof M,
        F & { member: string & (keyof D | keyof M) }
      >,
  ) {
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
