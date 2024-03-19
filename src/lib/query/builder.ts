/* eslint-disable @typescript-eslint/no-explicit-any */
import * as graphlib from "@dagrejs/graphlib";

import { AnyQuery, QuerySegment, TableQuery } from "../../types.js";
import type { AnyDatabase, Join } from "../builder/database.js";

import knex from "knex";
import invariant from "tiny-invariant";
import { BaseDialect } from "../dialect/base.js";
import { expandQueryToSegments } from "./expand-query.js";
import { findOptimalJoinGraph } from "./optimal-join-graph.js";

interface ReferencedTables {
  all: string[];
  dimensions: string[];
  metrics: string[];
}

const client = knex({ client: "pg" });

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: <explanation>
function buildQuerySegmentJoinQuery(
  knex: knex.Knex,
  database: AnyDatabase,
  Dialect: typeof BaseDialect,
  joinGraph: graphlib.Graph,
  tableQueries: Record<string, TableQuery>,
  source: string,
) {
  const visitedTables = new Set<string>();

  const sqlQuery = knex(source);
  const dialect = new Dialect(sqlQuery);

  const tableStack: { tableName: string; join?: Join }[] = [
    { tableName: source },
  ];

  while (tableStack.length > 0) {
    const { tableName, join } = tableStack.pop()!;
    if (visitedTables.has(tableName)) {
      continue;
    }
    visitedTables.add(tableName);

    const tableQuery = tableQueries[tableName];
    const table = database.getTable(tableName);
    const hasMetrics = tableQuery?.metrics && tableQuery.metrics.size > 0;
    const unvisitedNeighbors = (joinGraph.neighbors(tableName) ?? []).filter(
      (tableName) => !visitedTables.has(tableName),
    );
    const dimensionNames = new Set(tableQuery?.dimensions || []);

    if (hasMetrics) {
      for (const d of table.getPrimaryKeyDimensions()) {
        dimensionNames.add(d.getPath());
      }
    }

    if (join) {
      const joinType = join.reversed ? "rightJoin" : "leftJoin";
      const joinOn = join.joinOnDef.render(database, dialect);

      sqlQuery[joinType](join.right, knex.raw(joinOn.sql, joinOn.bindings));

      // We have a join that is multiplying the rows, so we need to use DISTINCT
      if (join.type === "manyToMany" || join.type === "oneToMany") {
        sqlQuery.distinct();
      }
    }

    for (const metricName of tableQuery?.metrics || []) {
      const metric = database.getMetric(metricName);
      const { sql, bindings } = metric.getSql(dialect);
      sqlQuery.select(
        knex.raw(`${sql} as ${metric.getAlias(dialect)}`, bindings),
      );
    }

    for (const dimensionName of dimensionNames) {
      const dimension = database.getDimension(dimensionName);
      const { sql, bindings } = dimension.getSql(dialect);

      sqlQuery.select(
        knex.raw(`${sql} as ${dimension.getAlias(dialect)}`, bindings),
      );
    }

    tableStack.push(
      ...unvisitedNeighbors.map((unvisitedTableName) => ({
        tableName: unvisitedTableName,
        join: database.getJoin(tableName, unvisitedTableName),
      })),
    );
  }

  return sqlQuery;
}

function buildQuerySegment(
  knex: knex.Knex,
  database: AnyDatabase,
  Dialect: typeof BaseDialect,
  joinGraph: graphlib.Graph,
  segment: QuerySegment,
) {
  const sources = joinGraph.sources();

  const source =
    segment.referencedTables.metrics.length > 0
      ? segment.referencedTables.metrics[0]
      : sources[0];

  invariant(source, "No source found for segment");

  const initialSqlQuery = buildQuerySegmentJoinQuery(
    knex,
    database,
    Dialect,
    joinGraph,
    segment.tableQueries,
    source,
  );
  const dialect = new Dialect(initialSqlQuery);

  if (segment.query.filters) {
    const filter = database
      .getFilterBuilder(
        database,
        dialect,
        "dimension",
        segment.referencedTables.all,
      )
      .buildFilters(segment.query.filters, "and");

    if (filter) {
      initialSqlQuery.where(knex.raw(filter.sql, filter.bindings));
    }
  }

  const alias = `${source}_query`;
  const sqlQuery = knex(initialSqlQuery.as(alias));
  const hasMetrics = segment.query.metrics && segment.query.metrics.length > 0;

  for (const dimensionName of segment.query.dimensions || []) {
    const dimension = database.getDimension(dimensionName);
    sqlQuery.select(
      knex.raw(
        `${dialect.asIdentifier(alias)}.${dimension.getAlias(
          dialect,
        )} as ${dimension.getAlias(dialect)}`,
      ),
    );
    if (hasMetrics) {
      sqlQuery.groupBy(
        knex.raw(
          `${dialect.asIdentifier(alias)}.${dimension.getAlias(dialect)}`,
        ),
      );
    }
  }

  for (const metricName of segment.query.metrics || []) {
    const metric = database.getMetric(metricName);
    const { sql, bindings } = metric.getAggregateSql(dialect, alias);

    sqlQuery.select(
      knex.raw(`${sql} as ${metric.getAlias(dialect)}`, bindings),
    );
  }

  return { ...segment, sqlQuery };
}

function getAlias(index: number) {
  return `q${index}`;
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: <explanation>
function buildQuery(
  knex: knex.Knex,
  database: AnyDatabase,
  Dialect: typeof BaseDialect,
  query: AnyQuery,
  referencedTables: ReferencedTables,
  joinGraph: graphlib.Graph,
  segments: QuerySegment[],
) {
  const sqlQuerySegments = segments.map((segment) =>
    buildQuerySegment(client, database, Dialect, joinGraph, segment),
  );
  const [initialSqlQuerySegment, ...restSqlQuerySegments] = sqlQuerySegments;

  invariant(initialSqlQuerySegment, "No initial sql query segment found");

  const joinOnDimensions = referencedTables.dimensions.flatMap((tableName) =>
    database.getTable(tableName).getPrimaryKeyDimensions(),
  );
  const rootAlias = getAlias(0);
  const rootSqlQuery = knex(initialSqlQuerySegment.sqlQuery.as(rootAlias));
  const dialect = new Dialect(rootSqlQuery);

  for (const dimensionName of initialSqlQuerySegment.projectedQuery
    .dimensions || []) {
    const dimension = database.getDimension(dimensionName);

    rootSqlQuery.select(
      knex.raw(
        `${dialect.asIdentifier(rootAlias)}.${dimension.getAlias(
          dialect,
        )} as ${dimension.getAlias(dialect)}`,
      ),
    );
  }

  for (const metricName of initialSqlQuerySegment.projectedQuery.metrics ||
    []) {
    const metric = database.getMetric(metricName);

    rootSqlQuery.select(
      knex.raw(
        `${dialect.asIdentifier(rootAlias)}.${metric.getAlias(
          dialect,
        )} as ${metric.getAlias(dialect)}`,
      ),
    );
  }

  for (let i = 0; i < restSqlQuerySegments.length; i++) {
    const segment = restSqlQuerySegments[i]!;
    const alias = getAlias(i + 1);
    const joinOn =
      (query.dimensions?.length ?? 0) > 0
        ? joinOnDimensions
            .map((dimension) => {
              return `${dialect.asIdentifier(rootAlias)}.${dimension.getAlias(
                dialect,
              )} = ${dialect.asIdentifier(alias)}.${dimension.getAlias(
                dialect,
              )}`;
            })
            .join(" and ")
        : "1 = 1";

    rootSqlQuery.innerJoin(segment.sqlQuery.as(alias), knex.raw(joinOn));

    for (const metricName of segment.projectedQuery.metrics || []) {
      if ((query.metrics ?? []).includes(metricName)) {
        const metric = database.getMetric(metricName);
        rootSqlQuery.select(
          knex.raw(
            `${dialect.asIdentifier(alias)}.${metric.getAlias(
              dialect,
            )} as ${metric.getAlias(dialect)}`,
          ),
        );
      }
    }
  }

  if (query.filters) {
    const metricPrefixes = sqlQuerySegments.reduce<Record<string, string>>(
      (acc, segment, idx) => {
        if (segment.metricTable) {
          acc[segment.metricTable] = getAlias(idx);
        }
        return acc;
      },
      {},
    );
    const filter = database
      .getFilterBuilder(
        database,
        dialect,
        "metric",
        referencedTables.metrics,
        metricPrefixes,
      )
      .buildFilters(query.filters, "and");
    if (filter) {
      rootSqlQuery.where(knex.raw(filter.sql, filter.bindings));
    }
  }

  const orderBy = Object.entries(query.order || {}).map(
    ([member, direction]) => {
      const memberSql = database.getMember(member).getAlias(dialect);
      return `${memberSql} ${direction}`;
    },
  );

  if (orderBy.length > 0) {
    rootSqlQuery.orderByRaw(orderBy.join(", "));
  }

  rootSqlQuery.limit(query.limit ?? 5000);
  rootSqlQuery.offset(query.offset ?? 0);

  return rootSqlQuery;
}

export function build(
  database: AnyDatabase,
  Dialect: typeof BaseDialect,
  query: AnyQuery,
) {
  const { referencedTables, segments } = expandQueryToSegments(database, query);

  const joinGraph = findOptimalJoinGraph(database.graph, referencedTables.all);

  const sqlQuery = buildQuery(
    client,
    database,
    Dialect,
    query,
    referencedTables,
    joinGraph,
    segments,
  );

  const result = sqlQuery.toSQL().toNative();
  const bindings: unknown[] = [...result.bindings];

  return {
    sql: result.sql,
    bindings,
  };
}
