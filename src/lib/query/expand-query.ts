import {
  AnyQuery,
  AnyQueryFilter,
  QuerySegment,
  TableQuery,
} from "../../types.js";

import { Database } from "../builder/database.js";

function analyzeQuery(database: Database, query: AnyQuery) {
  const allTables = new Set<string>();
  const dimensionTables = new Set<string>();
  const metricTables = new Set<string>();
  const projectedDimensionsByTable: Record<string, Set<string>> = {};
  const dimensionsByTable: Record<string, Set<string>> = {};
  const projectedMetricsByTable: Record<string, Set<string>> = {};
  const metricsByTable: Record<string, Set<string>> = {};

  for (const dimension of query.dimensions || []) {
    const tableName = database.getDimension(dimension).table.name;
    allTables.add(tableName);
    dimensionTables.add(tableName);
    dimensionsByTable[tableName] ||= new Set<string>();
    dimensionsByTable[tableName]!.add(dimension);
    projectedDimensionsByTable[tableName] ||= new Set<string>();
    projectedDimensionsByTable[tableName]!.add(dimension);
  }

  for (const metric of query.metrics || []) {
    const tableName = database.getMetric(metric).table.name;
    allTables.add(tableName);
    metricTables.add(tableName);
    metricsByTable[tableName] ||= new Set<string>();
    metricsByTable[tableName]!.add(metric);
    projectedMetricsByTable[tableName] ||= new Set<string>();
    projectedMetricsByTable[tableName]!.add(metric);
  }

  const filterStack: AnyQueryFilter[] = [...(query.filters || [])];

  while (filterStack.length > 0) {
    const filter = filterStack.pop()!;
    if (filter.operator === "and" || filter.operator === "or") {
      filterStack.push(...filter.filters);
    } else {
      const member = database.getMember(filter.member);
      const tableName = member.table.name;

      allTables.add(tableName);

      if (member.isDimension()) {
        // dimensionTables are used for join of query segments
        // so we're not adding them here, because we don't have
        // a guarantee that join on dimensions will be projected
        // (and if we projected them automatically, we'd get wrong results)
        // In the segment query allTables are used to join tables, which
        // means that any dimension filters will work
        dimensionsByTable[tableName] ||= new Set<string>();
        dimensionsByTable[tableName]!.add(filter.member);
      } else {
        metricTables.add(tableName);
        metricsByTable[tableName] ||= new Set<string>();
        metricsByTable[tableName]!.add(filter.member);
      }
    }
  }

  return {
    allTables,
    dimensionTables,
    metricTables,
    dimensionsByTable,
    projectedDimensionsByTable,
    metricsByTable,
    projectedMetricsByTable,
  };
}

interface PreparedQuery {
  dimensions: Set<string>;
  metrics: Set<string>;
  filters: [];
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: <explanation>
function getQuerySegment(
  database: Database,
  queryAnalysis: ReturnType<typeof analyzeQuery>,
  metricTable: string | null,
  index: number,
): QuerySegment {
  const queries: {
    query: PreparedQuery;
    projectedQuery: PreparedQuery;
  } = {
    query: {
      dimensions: new Set<string>(),
      metrics: new Set<string>(),
      filters: [],
    },
    projectedQuery: {
      dimensions: new Set<string>(),
      metrics: new Set<string>(),
      filters: [],
    },
  };

  const queriesKeys = Object.keys(queries) as (keyof typeof queries)[];

  const referencedTables = {
    all: new Set<string>(queryAnalysis.allTables),
    dimensions: new Set<string>(),
    metrics: new Set<string>(),
  };

  const tableQueries: Record<string, TableQuery> = {};

  for (const q of queriesKeys) {
    for (const [tableName, dimensions] of Object.entries(
      queryAnalysis.projectedDimensionsByTable,
    )) {
      const table = database.getTable(tableName);
      referencedTables.all.add(tableName);
      referencedTables.dimensions.add(tableName);

      const primaryKeyDimensionNames = table
        .getPrimaryKeyDimensions()
        .map((d) => d.getPath());

      if (index === 0) {
        for (const dimension of dimensions) {
          queries[q].dimensions.add(dimension);
        }
      }

      if (q === "query") {
        for (const dimension of primaryKeyDimensionNames) {
          queries[q].dimensions.add(dimension);
        }
      }

      tableQueries[tableName] = {
        dimensions: new Set<string>(
          index === 0
            ? new Set([...dimensions, ...primaryKeyDimensionNames])
            : new Set(primaryKeyDimensionNames),
        ),
        metrics: new Set<string>(),
      };
    }
  }

  if (metricTable) {
    referencedTables.all.add(metricTable);
    referencedTables.metrics.add(metricTable);
    tableQueries[metricTable] ||= {
      dimensions: new Set<string>(),
      metrics: new Set<string>(),
    };

    for (const q of queriesKeys) {
      const metrics = metricTable
        ? queryAnalysis[
            q === "query" ? "metricsByTable" : "projectedMetricsByTable"
          ][metricTable] ?? new Set<string>()
        : new Set<string>();
      for (const metric of metrics) {
        queries[q].metrics.add(metric);
        tableQueries[metricTable]!.metrics.add(metric);
      }
    }
  }

  return {
    query: {
      ...queries.query,
      dimensions: Array.from(queries.query.dimensions),
      metrics: Array.from(queries.query.metrics),
    },
    projectedQuery: {
      ...queries.projectedQuery,
      dimensions: Array.from(queries.projectedQuery.dimensions),
      metrics: Array.from(queries.projectedQuery.metrics),
    },
    referencedTables: {
      all: Array.from(referencedTables.all),
      dimensions: Array.from(referencedTables.dimensions),
      metrics: Array.from(referencedTables.metrics),
    },
    tableQueries,
    metricTable,
  };
}

function mergeQuerySegmentWithFilters(
  segment: QuerySegment,
  filters: AnyQueryFilter[] | undefined,
): QuerySegment {
  return {
    ...segment,
    query: {
      ...segment.query,
      filters: filters || [],
    },
    projectedQuery: {
      ...segment.projectedQuery,
      filters: filters || [],
    },
  };
}

export function expandQueryToSegments(database: Database, query: AnyQuery) {
  const queryAnalysis = analyzeQuery(database, query);
  const metricTables = Object.keys(queryAnalysis.metricsByTable);
  const segments =
    metricTables.length === 0
      ? [
          mergeQuerySegmentWithFilters(
            getQuerySegment(database, queryAnalysis, null, 0),
            query.filters,
          ),
        ]
      : metricTables.map((table, idx) =>
          mergeQuerySegmentWithFilters(
            getQuerySegment(database, queryAnalysis, table, idx),
            query.filters,
          ),
        );

  return {
    query,
    referencedTables: {
      all: Array.from(queryAnalysis.allTables),
      dimensions: Array.from(queryAnalysis.dimensionTables),
      metrics: Array.from(queryAnalysis.metricTables),
    },
    segments,
  };
}
