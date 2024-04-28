import * as graphlib from "@dagrejs/graphlib";

import {
  ModelQuery,
  Query,
  QueryAdHocMetric,
  QueryMetric,
  QuerySegment,
} from "../types.js";

import invariant from "tiny-invariant";
import { BaseDialect } from "../dialect/base.js";
import type { AnyJoin } from "../join.js";
import { AnyModel } from "../model.js";
import { AnyQueryBuilder } from "../query-builder.js";
import type { AnyRepository } from "../repository.js";
import { getAdHocAlias } from "../util.js";

interface ReferencedModels {
  all: string[];
  dimensions: string[];
  metrics: string[];
}

function getAdHocMetricAlias(adHocMetric: QueryAdHocMetric) {
  return getAdHocAlias(adHocMetric.dimension, adHocMetric.aggregateWith);
}

function getSortableMetric(metrics: QueryMetric[] | undefined) {
  for (const metric of metrics ?? []) {
    if (typeof metric === "string") {
      return metric;
    }
  }
}

function getDefaultOrderBy(repository: AnyRepository, query: Query) {
  const firstDimensionName = query.dimensions?.[0];
  const firstMetricName = getSortableMetric(query.metrics);

  for (const dimensionName of query.dimensions ?? []) {
    const dimension = repository.getDimension(dimensionName);
    if (dimension.getGranularity()) {
      return { [dimensionName]: "asc" };
    }
  }

  if (firstMetricName) {
    return { [firstMetricName]: "desc" };
  }

  if (firstDimensionName) {
    return { [firstDimensionName]: "asc" };
  }

  return {};
}

function initializeQuerySegment(
  dialect: BaseDialect,
  context: unknown,
  model: AnyModel,
) {
  if (model.config.type === "table") {
    const { sql, bindings } = model.getTableName(dialect, context);
    return dialect.from(dialect.fragment(sql, bindings));
  }
  const modelSql = model.getSql(dialect, context);
  return dialect.from(
    dialect.fragment(
      `(${modelSql.sql}) as ${dialect.asIdentifier(model.config.alias)}`,
      modelSql.bindings,
    ),
  );
}

function getJoinSubject(
  dialect: BaseDialect,
  context: unknown,
  model: AnyModel,
) {
  if (model.config.type === "table") {
    const { sql, bindings } = model.getTableName(dialect, context);
    return dialect.fragment(sql, bindings);
  }

  const modelSql = model.getSql(dialect, context);
  return dialect.fragment(
    `(${modelSql.sql}) as ${dialect.asIdentifier(model.config.alias)}`,
    modelSql.bindings,
  );
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Essential complexity
function buildQuerySegmentJoinQuery(
  queryBuilder: AnyQueryBuilder,
  context: unknown,
  joinGraph: graphlib.Graph,
  modelQueries: Record<string, ModelQuery>,
  source: string,
) {
  const visitedModels = new Set<string>();
  const model = queryBuilder.repository.getModel(source);
  const sqlQuery = initializeQuerySegment(queryBuilder.dialect, context, model);

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
    const hasAdHocMetrics =
      modelQuery?.adHocMetrics && modelQuery.adHocMetrics.size > 0;
    const unvisitedNeighbors = (joinGraph.neighbors(modelName) ?? []).filter(
      (modelName) => !visitedModels.has(modelName),
    );
    const dimensionNames = new Set(modelQuery?.dimensions || []);

    if (hasMetrics || hasAdHocMetrics) {
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
      const joinSubject = getJoinSubject(
        queryBuilder.dialect,
        context,
        rightModel,
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
      const { sql, bindings } = metric.getSql(queryBuilder.dialect, context);
      sqlQuery.select(
        queryBuilder.dialect.fragment(
          `${sql} as ${metric.getAlias(queryBuilder.dialect)}`,
          bindings,
        ),
      );
    }

    for (const adHocMetric of modelQuery?.adHocMetrics || []) {
      const dimension = queryBuilder.repository.getDimension(
        adHocMetric.dimension,
      );
      const { sql, bindings } = dimension.getSqlWithoutGranularity(
        queryBuilder.dialect,
        context,
      );
      sqlQuery.select(
        queryBuilder.dialect.fragment(
          `${sql} as ${getAdHocMetricAlias(adHocMetric)}`,
          bindings,
        ),
      );
    }

    for (const dimensionName of dimensionNames) {
      const dimension = queryBuilder.repository.getDimension(dimensionName);
      const { sql, bindings } = dimension.getSql(queryBuilder.dialect, context);

      sqlQuery.select(
        queryBuilder.dialect.fragment(
          `${sql} as ${dimension.getAlias(queryBuilder.dialect)}`,
          bindings,
        ),
      );
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
function buildQuerySegment(
  queryBuilder: AnyQueryBuilder,
  context: unknown,
  joinGraph: graphlib.Graph,
  segment: QuerySegment,
) {
  const sources = joinGraph.sources();

  const source =
    segment.referencedModels.metrics.length > 0
      ? segment.referencedModels.metrics[0]
      : sources[0];

  invariant(source, "No source found for segment");

  const initialSqlQuery = buildQuerySegmentJoinQuery(
    queryBuilder,
    context,
    joinGraph,
    segment.modelQueries,
    source,
  );

  // If there are no metrics, we need to use DISTINCT to avoid multiplying rows
  // otherwise GROUP BY will take care of it
  if (
    (segment.query.metrics?.length ?? 0) === 0 &&
    (segment.query.adHocMetrics?.length ?? 0) === 0
  ) {
    initialSqlQuery.distinct();
  }

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

  const alias = `${source}_query`;
  const sqlQuery = queryBuilder.dialect.from(initialSqlQuery.as(alias));
  const hasMetrics =
    (segment.query.metrics && segment.query.metrics.length > 0) ||
    (segment.query.adHocMetrics && segment.query.adHocMetrics.length > 0);

  for (const dimensionName of segment.query.dimensions || []) {
    const dimension = queryBuilder.repository.getDimension(dimensionName);
    sqlQuery.select(
      queryBuilder.dialect.fragment(
        `${queryBuilder.dialect.asIdentifier(alias)}.${dimension.getAlias(
          queryBuilder.dialect,
        )} as ${dimension.getAlias(queryBuilder.dialect)}`,
      ),
    );
    if (hasMetrics) {
      sqlQuery.groupBy(
        queryBuilder.dialect.fragment(
          `${queryBuilder.dialect.asIdentifier(alias)}.${dimension.getAlias(
            queryBuilder.dialect,
          )}`,
        ),
      );
    }
  }

  for (const metricName of segment.query.metrics || []) {
    const metric = queryBuilder.repository.getMetric(metricName);
    const { sql, bindings } = metric.getAggregateSql(
      queryBuilder.dialect,
      context,
      alias,
    );

    sqlQuery.select(
      queryBuilder.dialect.fragment(
        `${sql} as ${metric.getAlias(queryBuilder.dialect)}`,
        bindings,
      ),
    );
  }

  for (const adHocMetric of segment.query.adHocMetrics || []) {
    const dimension = queryBuilder.repository.getDimension(
      adHocMetric.dimension,
    );
    const initialSql = queryBuilder.dialect.aggregate(
      adHocMetric.aggregateWith,
      `${queryBuilder.dialect.asIdentifier(alias)}.${getAdHocMetricAlias(
        adHocMetric,
      )}`,
    );
    const dimensionGranularity = dimension.getGranularity();
    const sql = dimensionGranularity
      ? queryBuilder.dialect.withGranularity(dimensionGranularity, initialSql)
      : initialSql;

    sqlQuery.select(
      queryBuilder.dialect.fragment(
        `${sql} as ${getAdHocMetricAlias(adHocMetric)}`,
      ),
    );
  }

  return { ...segment, sqlQuery };
}

function getAlias(index: number) {
  return `q${index}`;
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: <explanation>
export function buildQuery(
  queryBuilder: AnyQueryBuilder,
  context: unknown,
  query: Query,
  referencedModels: ReferencedModels,
  joinGraph: graphlib.Graph,
  segments: QuerySegment[],
) {
  const sqlQuerySegments = segments.map((segment) =>
    buildQuerySegment(queryBuilder, context, joinGraph, segment),
  );
  const [initialSqlQuerySegment, ...restSqlQuerySegments] = sqlQuerySegments;

  invariant(initialSqlQuerySegment, "No initial sql query segment found");

  /*const joinOnDimensions = referencedModels.dimensions.flatMap((modelName) =>
    repository.getModel(modelName).getPrimaryKeyDimensions(),
  );*/

  const joinOnDimensions = query.dimensions?.map((dimensionName) => {
    return queryBuilder.repository.getDimension(dimensionName);
  });

  const rootAlias = getAlias(0);
  const rootSqlQuery = queryBuilder.dialect.from(
    initialSqlQuerySegment.sqlQuery.as(rootAlias),
  );

  for (const dimensionName of initialSqlQuerySegment.projectedQuery
    .dimensions || []) {
    const dimension = queryBuilder.repository.getDimension(dimensionName);

    rootSqlQuery.select(
      queryBuilder.dialect.fragment(
        `${queryBuilder.dialect.asIdentifier(rootAlias)}.${dimension.getAlias(
          queryBuilder.dialect,
        )} as ${dimension.getAlias(queryBuilder.dialect)}`,
      ),
    );
  }

  for (const metricName of initialSqlQuerySegment.projectedQuery.metrics ||
    []) {
    const metric = queryBuilder.repository.getMetric(metricName);

    rootSqlQuery.select(
      queryBuilder.dialect.fragment(
        `${queryBuilder.dialect.asIdentifier(rootAlias)}.${metric.getAlias(
          queryBuilder.dialect,
        )} as ${metric.getAlias(queryBuilder.dialect)}`,
      ),
    );
  }

  for (const adHocMetric of initialSqlQuerySegment.projectedQuery
    .adHocMetrics || []) {
    rootSqlQuery.select(
      queryBuilder.dialect.fragment(
        `${queryBuilder.dialect.asIdentifier(rootAlias)}.${getAdHocMetricAlias(
          adHocMetric,
        )} as ${getAdHocMetricAlias(adHocMetric)}`,
      ),
    );
  }

  for (let i = 0; i < restSqlQuerySegments.length; i++) {
    const segment = restSqlQuerySegments[i]!;
    const alias = getAlias(i + 1);
    const joinOn =
      joinOnDimensions && joinOnDimensions.length > 0
        ? joinOnDimensions
            .map((dimension) => {
              return `${queryBuilder.dialect.asIdentifier(
                rootAlias,
              )}.${dimension.getAlias(
                queryBuilder.dialect,
              )} = ${queryBuilder.dialect.asIdentifier(
                alias,
              )}.${dimension.getAlias(queryBuilder.dialect)}`;
            })
            .join(" and ")
        : "1 = 1";

    rootSqlQuery.innerJoin(
      segment.sqlQuery.as(alias),
      queryBuilder.dialect.fragment(joinOn),
    );

    for (const metricName of segment.projectedQuery.metrics || []) {
      if ((query.metrics ?? []).includes(metricName)) {
        const metric = queryBuilder.repository.getMetric(metricName);
        rootSqlQuery.select(
          queryBuilder.dialect.fragment(
            `${queryBuilder.dialect.asIdentifier(alias)}.${metric.getAlias(
              queryBuilder.dialect,
            )} as ${metric.getAlias(queryBuilder.dialect)}`,
          ),
        );
      }
    }

    for (const adHocMetric of segment.projectedQuery.adHocMetrics || []) {
      rootSqlQuery.select(
        queryBuilder.dialect.fragment(
          `${queryBuilder.dialect.asIdentifier(alias)}.${getAdHocMetricAlias(
            adHocMetric,
          )} as ${getAdHocMetricAlias(adHocMetric)}`,
        ),
      );
    }
  }

  if (query.filters) {
    const metricPrefixes = sqlQuerySegments.reduce<Record<string, string>>(
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

  const orderBy = Object.entries(
    query.order || getDefaultOrderBy(queryBuilder.repository, query),
  ).map(([member, direction]) => {
    const memberSql = queryBuilder.repository
      .getMember(member)
      .getAlias(queryBuilder.dialect);
    return `${memberSql} ${direction}`;
  });

  if (orderBy.length > 0) {
    rootSqlQuery.orderBy(orderBy.join(", "));
  }

  rootSqlQuery.limit(query.limit ?? 5000);
  rootSqlQuery.offset(query.offset ?? 0);

  return rootSqlQuery;
}
