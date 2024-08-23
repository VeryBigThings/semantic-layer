import * as graphlib from "@dagrejs/graphlib";

import { ModelQuery, Order, Query, QuerySegment } from "../types.js";

import invariant from "tiny-invariant";
import type { AnyJoin } from "../join.js";
import { AnyQueryBuilder } from "../query-builder.js";
import type { AnyRepository } from "../repository.js";

interface ReferencedModels {
  all: string[];
  dimensions: string[];
  metrics: string[];
}

function getDefaultOrderBy(repository: AnyRepository, query: Query): Order[] {
  const firstDimensionName = query.dimensions?.[0];
  const firstMetricName = query.metrics?.[0];

  for (const dimensionName of query.dimensions ?? []) {
    const dimension = repository.getDimension(dimensionName);
    if (dimension.getGranularity()) {
      return [{ member: dimensionName, direction: "asc" }];
    }
  }

  if (firstMetricName) {
    return [{ member: firstMetricName, direction: "desc" }];
  }

  if (firstDimensionName) {
    return [{ member: firstDimensionName, direction: "asc" }];
  }

  return [];
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Essential complexity
function buildModelQuery(
  queryBuilder: AnyQueryBuilder,
  context: unknown,
  joinGraph: graphlib.Graph,
  modelQueries: Record<string, ModelQuery>,
  source: string,
) {
  const visitedModels = new Set<string>();
  const model = queryBuilder.repository.getModel(source);
  const sqlQuery = queryBuilder.dialect.from(
    model.getTableNameOrSql(
      queryBuilder.repository,
      queryBuilder.dialect,
      context,
    ),
  );

  const modelStack: { modelName: string; join?: AnyJoin }[] = [
    { modelName: source },
  ];

  while (modelStack.length > 0) {
    const { modelName, join } = modelStack.pop()!;
    if (visitedModels.has(modelName)) {
      continue;
    }
    visitedModels.add(modelName);

    const modelQuery = modelQueries[modelName];
    const model = queryBuilder.repository.getModel(modelName);
    const hasMetrics = modelQuery?.metrics && modelQuery.metrics.size > 0;
    const unvisitedNeighbors = (joinGraph.neighbors(modelName) ?? []).filter(
      (modelName) => !visitedModels.has(modelName),
    );
    const dimensionNames = new Set(modelQuery?.dimensions || []);

    if (hasMetrics) {
      for (const d of model.getPrimaryKeyDimensions()) {
        dimensionNames.add(d.getPath());
      }
    }

    if (join) {
      const joinType = join.reversed ? "rightJoin" : "leftJoin";
      const joinOn = join
        .joinOnDef(context)
        .render(queryBuilder.repository, queryBuilder.dialect);
      const rightModel = queryBuilder.repository.getModel(join.right);
      const joinSubject = rightModel.getTableNameOrSql(
        queryBuilder.repository,
        queryBuilder.dialect,
        context,
      );

      sqlQuery[joinType](
        joinSubject,
        queryBuilder.dialect.fragment(joinOn.sql, joinOn.bindings),
      );

      // We have a join that is multiplying the rows, so we need to use DISTINCT
      if (join.type === "manyToMany" || join.type === "oneToMany") {
        sqlQuery.distinct();
      }
    }

    for (const metricName of modelQuery?.metrics || []) {
      const metric = queryBuilder.repository.getMetric(metricName);
      const modelQueryProjection = metric.getModelQueryProjection(
        queryBuilder.repository,
        queryBuilder.dialect,
        context,
      );

      for (const fragment of modelQueryProjection) {
        sqlQuery.select(fragment);
      }
    }

    for (const dimensionName of dimensionNames) {
      const dimension = queryBuilder.repository.getDimension(dimensionName);
      const modelQueryProjection = dimension.getModelQueryProjection(
        queryBuilder.repository,
        queryBuilder.dialect,
        context,
      );

      for (const fragment of modelQueryProjection) {
        sqlQuery.select(fragment);
      }
    }

    modelStack.push(
      ...unvisitedNeighbors.map((unvisitedModelName) => ({
        modelName: unvisitedModelName,
        join: queryBuilder.repository.getJoin(modelName, unvisitedModelName),
      })),
    );
  }

  return sqlQuery;
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Essential complexity
function buildSegmentQuery(
  queryBuilder: AnyQueryBuilder,
  context: unknown,
  joinGraph: graphlib.Graph,
  segment: QuerySegment,
  overrideModelQueryAlias?: string,
) {
  const sources = joinGraph.sources();

  const source =
    segment.referencedModels.metrics.length > 0
      ? segment.referencedModels.metrics[0]
      : sources[0];

  invariant(source, "No source found for segment");

  const modelQueryAlias = overrideModelQueryAlias ?? `${source}_query`;

  const initialSqlQuery = buildModelQuery(
    queryBuilder,
    context,
    joinGraph,
    segment.modelQueries,
    source,
  );

  if (segment.query.filters) {
    const filter = queryBuilder
      .getFilterBuilder("dimension", segment.referencedModels.all)
      .buildFilters(segment.query.filters, "and", context);

    if (filter) {
      initialSqlQuery.where(
        queryBuilder.dialect.fragment(filter.sql, filter.bindings),
      );
    }
  }

  /* Handle the case where there are no metrics - we shouldn't wrap the query in a sub query, but we need to figure out the aliases
	const hasMetrics = segment.query.metrics && segment.query.metrics.length > 0;*/

  const sqlQuery = queryBuilder.dialect.from(
    initialSqlQuery.as(modelQueryAlias),
  );

  for (const dimensionName of segment.query.dimensions || []) {
    const dimension = queryBuilder.repository.getDimension(dimensionName);
    const segmentQueryProjection = dimension.getSegmentQueryProjection(
      queryBuilder.repository,
      queryBuilder.dialect,
      context,
      modelQueryAlias,
    );

    for (const fragment of segmentQueryProjection) {
      sqlQuery.select(fragment);
    }

    // Always GROUP BY by the dimensions, if there are no metrics, it will behave as DISTINCT
    const segmentQueryGroupBy = dimension.getSegmentQueryGroupBy(
      queryBuilder.repository,
      queryBuilder.dialect,
      context,
      modelQueryAlias,
    );

    for (const fragment of segmentQueryGroupBy) {
      sqlQuery.groupBy(fragment);
    }
  }

  for (const metricName of segment.query.metrics || []) {
    const metric = queryBuilder.repository.getMetric(metricName);
    const segmentQueryProjection = metric.getSegmentQueryProjection(
      queryBuilder.repository,
      queryBuilder.dialect,
      context,
      modelQueryAlias,
    );

    for (const fragment of segmentQueryProjection) {
      sqlQuery.select(fragment);
    }

    const segmentQueryGroupBy = metric.getSegmentQueryGroupBy(
      queryBuilder.repository,
      queryBuilder.dialect,
      context,
      modelQueryAlias,
    );

    for (const fragment of segmentQueryGroupBy) {
      sqlQuery.groupBy(fragment);
    }
  }

  return { ...segment, sqlQuery };
}

function getAlias(index: number) {
  return `q${index}`;
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Essential complexity
export function buildRootQuery(
  queryBuilder: AnyQueryBuilder,
  context: unknown,
  query: Query,
  joinGraph: graphlib.Graph,
  segments: QuerySegment[],
) {
  if (segments.length === 1) {
    const sqlQuerySegment = buildSegmentQuery(
      queryBuilder,
      context,
      joinGraph,
      segments[0]!,
      getAlias(0),
    );

    return sqlQuerySegment.sqlQuery;
  }

  const sqlQuerySegments = segments.map((segment) =>
    buildSegmentQuery(queryBuilder, context, joinGraph, segment),
  );
  const [initialSqlQuerySegment, ...restSqlQuerySegments] = sqlQuerySegments;

  invariant(initialSqlQuerySegment, "No initial sql query segment found");

  const joinOnDimensions = query.dimensions?.map((dimensionName) => {
    return queryBuilder.repository.getDimension(dimensionName);
  });

  const rootQueryAlias = getAlias(0);
  const rootSqlQuery = queryBuilder.dialect.from(
    initialSqlQuerySegment.sqlQuery.as(rootQueryAlias),
  );

  for (const dimensionName of initialSqlQuerySegment.projectedQuery
    .dimensions || []) {
    const dimension = queryBuilder.repository.getDimension(dimensionName);
    const rootQueryProjection = dimension.getRootQueryProjection(
      queryBuilder.repository,
      queryBuilder.dialect,
      context,
      rootQueryAlias,
    );

    for (const fragment of rootQueryProjection) {
      rootSqlQuery.select(fragment);
    }
  }

  for (const metricName of initialSqlQuerySegment.projectedQuery.metrics ||
    []) {
    const metric = queryBuilder.repository.getMetric(metricName);

    const rootQueryProjection = metric.getRootQueryProjection(
      queryBuilder.repository,
      queryBuilder.dialect,
      context,
      rootQueryAlias,
    );

    for (const fragment of rootQueryProjection) {
      rootSqlQuery.select(fragment);
    }
  }

  for (let i = 0; i < restSqlQuerySegments.length; i++) {
    const segment = restSqlQuerySegments[i]!;
    const segmentQueryAlias = getAlias(i + 1);
    const joinOn =
      joinOnDimensions && joinOnDimensions.length > 0
        ? joinOnDimensions
            .map((dimension) => {
              const quotedRootQueryAlias =
                queryBuilder.dialect.asIdentifier(rootQueryAlias);
              const quotedSegmentQueryAlias =
                queryBuilder.dialect.asIdentifier(segmentQueryAlias);
              const quotedDimensionAlias = queryBuilder.dialect.asIdentifier(
                dimension.getAlias(),
              );

              return `${quotedRootQueryAlias}.${quotedDimensionAlias} = ${quotedSegmentQueryAlias}.${quotedDimensionAlias}`;
            })
            .join(" and ")
        : "1 = 1";

    rootSqlQuery.innerJoin(
      segment.sqlQuery.as(segmentQueryAlias),
      queryBuilder.dialect.fragment(joinOn),
    );

    for (const metricName of segment.projectedQuery.metrics || []) {
      if ((query.metrics ?? []).includes(metricName)) {
        const metric = queryBuilder.repository.getMetric(metricName);
        const rootQueryProjection = metric.getRootQueryProjection(
          queryBuilder.repository,
          queryBuilder.dialect,
          context,
          segmentQueryAlias,
        );

        for (const fragment of rootQueryProjection) {
          rootSqlQuery.select(fragment);
        }
      }
    }
  }
  return rootSqlQuery;
}

export function buildQuery(
  queryBuilder: AnyQueryBuilder,
  context: unknown,
  query: Query,
  referencedModels: ReferencedModels,
  joinGraph: graphlib.Graph,
  segments: QuerySegment[],
) {
  const rootSqlQuery = buildRootQuery(
    queryBuilder,
    context,
    query,
    joinGraph,
    segments,
  );

  if (query.filters) {
    const metricPrefixes = segments.reduce<Record<string, string>>(
      (acc, segment, idx) => {
        if (segment.metricModel) {
          acc[segment.metricModel] = getAlias(idx);
        }
        return acc;
      },
      {},
    );
    const filter = queryBuilder
      .getFilterBuilder("metric", referencedModels.metrics, metricPrefixes)
      .buildFilters(query.filters, "and", context);

    if (filter) {
      rootSqlQuery.where(
        queryBuilder.dialect.fragment(filter.sql, filter.bindings),
      );
    }
  }

  const orderBy = (
    query.order || getDefaultOrderBy(queryBuilder.repository, query)
  ).map(({ member, direction }) => {
    const quotedMemberAlias = queryBuilder.dialect.asIdentifier(
      queryBuilder.repository.getMember(member).getAlias(),
    );
    return `${quotedMemberAlias} ${direction}`;
  });

  if (orderBy.length > 0) {
    rootSqlQuery.orderBy(orderBy.join(", "));
  }

  rootSqlQuery.limit(query.limit ?? 5000);
  rootSqlQuery.offset(query.offset ?? 0);

  return rootSqlQuery;
}
