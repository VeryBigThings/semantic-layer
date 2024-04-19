import * as graphlib from "@dagrejs/graphlib";

import {
  AnyQuery,
  ModelQuery,
  QueryAdHocMetric,
  QueryMetric,
  QuerySegment,
} from "../types.js";

import knex from "knex";
import invariant from "tiny-invariant";
import { BaseDialect } from "../dialect/base.js";
import type { AnyJoin } from "../join.js";
import { AnyModel } from "../model.js";
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

function getDefaultOrderBy(repository: AnyRepository, query: AnyQuery) {
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
  knex: knex.Knex,
  dialect: BaseDialect,
  context: unknown,
  model: AnyModel,
) {
  if (model.config.type === "table") {
    return knex(model.config.name);
  }
  const modelSql = model.getSql(dialect, context);
  return knex(
    knex.raw(`(${modelSql.sql}) as ${model.config.alias}`, modelSql.bindings),
  );
}

function getJoinSubject(
  knex: knex.Knex,
  dialect: BaseDialect,
  context: unknown,
  model: AnyModel,
) {
  if (model.config.type === "table") {
    return model.config.name;
  }
  const modelSql = model.getSql(dialect, context);
  return knex.raw(
    `(${modelSql.sql}) as ${model.config.alias}`,
    modelSql.bindings,
  );
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Essential complexity
function buildQuerySegmentJoinQuery(
  knex: knex.Knex,
  repository: AnyRepository,
  dialect: BaseDialect,
  context: unknown,
  joinGraph: graphlib.Graph,
  modelQueries: Record<string, ModelQuery>,
  source: string,
) {
  const visitedModels = new Set<string>();
  const model = repository.getModel(source);
  const sqlQuery = initializeQuerySegment(knex, dialect, context, model);

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
    const model = repository.getModel(modelName);
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
      const joinOn = join.joinOnDef(context).render(repository, dialect);
      const rightModel = repository.getModel(join.right);
      const joinSubject = getJoinSubject(knex, dialect, context, rightModel);

      sqlQuery[joinType](joinSubject, knex.raw(joinOn.sql, joinOn.bindings));

      // We have a join that is multiplying the rows, so we need to use DISTINCT
      if (join.type === "manyToMany" || join.type === "oneToMany") {
        sqlQuery.distinct();
      }
    }

    for (const metricName of modelQuery?.metrics || []) {
      const metric = repository.getMetric(metricName);
      const { sql, bindings } = metric.getSql(dialect, context);
      sqlQuery.select(
        knex.raw(`${sql} as ${metric.getAlias(dialect)}`, bindings),
      );
    }

    for (const adHocMetric of modelQuery?.adHocMetrics || []) {
      const dimension = repository.getDimension(adHocMetric.dimension);
      const { sql, bindings } = dimension.getSqlWithoutGranularity(
        dialect,
        context,
      );
      sqlQuery.select(
        knex.raw(`${sql} as ${getAdHocMetricAlias(adHocMetric)}`, bindings),
      );
    }

    for (const dimensionName of dimensionNames) {
      const dimension = repository.getDimension(dimensionName);
      const { sql, bindings } = dimension.getSql(dialect, context);

      sqlQuery.select(
        knex.raw(`${sql} as ${dimension.getAlias(dialect)}`, bindings),
      );
    }

    modelStack.push(
      ...unvisitedNeighbors.map((unvisitedModelName) => ({
        modelName: unvisitedModelName,
        join: repository.getJoin(modelName, unvisitedModelName),
      })),
    );
  }

  return sqlQuery;
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Essential complexity
function buildQuerySegment(
  knex: knex.Knex,
  repository: AnyRepository,
  dialect: BaseDialect,
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
    knex,
    repository,
    dialect,
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
    const filter = repository
      .getFilterBuilder(
        repository,
        dialect,
        "dimension",
        segment.referencedModels.all,
      )
      .buildFilters(segment.query.filters, "and", context);

    if (filter) {
      initialSqlQuery.where(knex.raw(filter.sql, filter.bindings));
    }
  }

  const alias = `${source}_query`;
  const sqlQuery = knex(initialSqlQuery.as(alias));
  const hasMetrics =
    (segment.query.metrics && segment.query.metrics.length > 0) ||
    (segment.query.adHocMetrics && segment.query.adHocMetrics.length > 0);

  for (const dimensionName of segment.query.dimensions || []) {
    const dimension = repository.getDimension(dimensionName);
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
    const metric = repository.getMetric(metricName);
    const { sql, bindings } = metric.getAggregateSql(dialect, context, alias);

    sqlQuery.select(
      knex.raw(`${sql} as ${metric.getAlias(dialect)}`, bindings),
    );
  }

  for (const adHocMetric of segment.query.adHocMetrics || []) {
    const dimension = repository.getDimension(adHocMetric.dimension);
    const initialSql = dialect.aggregate(
      adHocMetric.aggregateWith,
      `${dialect.asIdentifier(alias)}.${getAdHocMetricAlias(adHocMetric)}`,
    );
    const dimensionGranularity = dimension.getGranularity();
    const sql = dimensionGranularity
      ? dialect.withGranularity(dimensionGranularity, initialSql)
      : initialSql;

    sqlQuery.select(knex.raw(`${sql} as ${getAdHocMetricAlias(adHocMetric)}`));
  }

  return { ...segment, sqlQuery };
}

function getAlias(index: number) {
  return `q${index}`;
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: <explanation>
export function buildQuery(
  knex: knex.Knex,
  repository: AnyRepository,
  dialect: BaseDialect,
  context: unknown,
  query: AnyQuery,
  referencedModels: ReferencedModels,
  joinGraph: graphlib.Graph,
  segments: QuerySegment[],
) {
  const sqlQuerySegments = segments.map((segment) =>
    buildQuerySegment(knex, repository, dialect, context, joinGraph, segment),
  );
  const [initialSqlQuerySegment, ...restSqlQuerySegments] = sqlQuerySegments;

  invariant(initialSqlQuerySegment, "No initial sql query segment found");

  /*const joinOnDimensions = referencedModels.dimensions.flatMap((modelName) =>
    repository.getModel(modelName).getPrimaryKeyDimensions(),
  );*/

  const joinOnDimensions = query.dimensions?.map((dimensionName) => {
    return repository.getDimension(dimensionName);
  });

  const rootAlias = getAlias(0);
  const rootSqlQuery = knex(initialSqlQuerySegment.sqlQuery.as(rootAlias));

  for (const dimensionName of initialSqlQuerySegment.projectedQuery
    .dimensions || []) {
    const dimension = repository.getDimension(dimensionName);

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
    const metric = repository.getMetric(metricName);

    rootSqlQuery.select(
      knex.raw(
        `${dialect.asIdentifier(rootAlias)}.${metric.getAlias(
          dialect,
        )} as ${metric.getAlias(dialect)}`,
      ),
    );
  }

  for (const adHocMetric of initialSqlQuerySegment.projectedQuery
    .adHocMetrics || []) {
    rootSqlQuery.select(
      knex.raw(
        `${dialect.asIdentifier(rootAlias)}.${getAdHocMetricAlias(
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
        const metric = repository.getMetric(metricName);
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
        if (segment.metricModel) {
          acc[segment.metricModel] = getAlias(idx);
        }
        return acc;
      },
      {},
    );
    const filter = repository
      .getFilterBuilder(
        repository,
        dialect,
        "metric",
        referencedModels.metrics,
        metricPrefixes,
      )
      .buildFilters(query.filters, "and", context);
    if (filter) {
      rootSqlQuery.where(knex.raw(filter.sql, filter.bindings));
    }
  }

  const orderBy = Object.entries(
    query.order || getDefaultOrderBy(repository, query),
  ).map(([member, direction]) => {
    const memberSql = repository.getMember(member).getAlias(dialect);
    return `${memberSql} ${direction}`;
  });

  if (orderBy.length > 0) {
    rootSqlQuery.orderByRaw(orderBy.join(", "));
  }

  rootSqlQuery.limit(query.limit ?? 5000);
  rootSqlQuery.offset(query.offset ?? 0);

  return rootSqlQuery;
}
