import { SqlFragment, SqlQueryBuilder } from "../sql-builder.js";
import { AnyInputQuery, Order } from "../types.js";
import { QueryPlan, getQueryPlan } from "./query-plan.js";

import invariant from "tiny-invariant";
import type { AnyJoin } from "../join.js";
import { AnyQueryBuilder } from "../query-builder.js";
import type { AnyRepository } from "../repository.js";
import { METRIC_REF_SUBQUERY_ALIAS } from "../util.js";

function getDefaultOrderBy(
  repository: AnyRepository,
  query: QueryPlan,
): Order[] {
  const firstDimensionName = query.projectedDimensions?.[0];
  const firstMetricName = query.projectedMetrics?.[0];

  for (const dimensionName of query.projectedDimensions ?? []) {
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

function getAlias(index: number) {
  return `q${index}`;
}

function arrayHasAtLeastOneElement<T>(value: T[]): value is [T, ...T[]] {
  return value.length > 0;
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: <explanation>
function buildModelQuery(
  queryBuilder: AnyQueryBuilder,
  context: unknown,
  segment: QueryPlan["segments"][number],
) {
  const visitedModels = new Set<string>();
  const model = queryBuilder.repository.getModel(segment.initialModel);
  const sqlQuery = queryBuilder.dialect.from(
    model.getTableNameOrSql(
      queryBuilder.repository,
      queryBuilder.dialect,
      context,
    ),
  );

  for (const memberName of segment.modelQuery.members) {
    const member = queryBuilder.repository.getMember(memberName);
    const modelQueryProjection = member.getModelQueryProjection(
      queryBuilder.repository,
      queryBuilder.dialect,
      context,
    );

    for (const fragment of modelQueryProjection) {
      sqlQuery.select(fragment);
    }
  }

  // Do the joins first, because we might have additional joins if there are metric refs, and they need to be able to reference the dimensions of the joins
  const modelsToProcess: { modelName: string; join?: AnyJoin }[] = [
    { modelName: segment.initialModel },
  ];

  while (modelsToProcess.length > 0) {
    const { modelName, join } = modelsToProcess.pop()!;
    if (visitedModels.has(modelName)) {
      continue;
    }
    visitedModels.add(modelName);

    const unvisitedNeighbors = (
      segment.joinGraph.neighbors(modelName) ?? []
    ).filter((modelName) => !visitedModels.has(modelName));

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

    modelsToProcess.push(
      ...unvisitedNeighbors.map((unvisitedModelName) => ({
        modelName: unvisitedModelName,
        join: queryBuilder.repository.getJoin(modelName, unvisitedModelName),
      })),
    );

    const metricRefs = Array.from(
      new Set(
        segment.modelQuery.metrics.flatMap((metricName) => {
          const metric = queryBuilder.repository.getMetric(metricName);
          return metric
            .getMetricRefs(context)
            .map((metricRef) => metricRef.metric.getPath());
        }),
      ),
    );

    /*if (modelQueryMetricRefsSubQuery) {
    const alias = `${pathToAlias(memberName)}___metric_refs_subquery`;
    const { sql, bindings } = modelQueryMetricRefsSubQuery.toSQL();
    console.log(sql);
    console.log("---------------------------------");
    sqlQuery.leftJoin(
      new SqlFragment(`(${sql}) as ${alias}`, bindings),
      filteredDimensions
        .map((dimensionPath) => {
          const dimension = queryBuilder.repository.getDimension(dimensionPath);

          return `${dimension.getSql(queryBuilder.repository, queryBuilder.dialect, context).sql} = ${alias}.${dimension.getAlias()}`;
        })
        .join(" and "),
    );
  }*/

    if (metricRefs.length > 0) {
      const query: AnyInputQuery = {
        members: [...segment.modelQuery.dimensions, ...metricRefs],
        filters: segment.filters,
      };
      const queryPlan = getQueryPlan(queryBuilder.repository, query);
      const { sql, bindings } = buildQuery(
        queryBuilder,
        context,
        queryPlan,
      ).toSQL();

      const joinOn = segment.modelQuery.dimensions.reduce<{
        sqls: string[];
        bindings: unknown[];
      }>(
        (acc, dimensionPath) => {
          const dimension = queryBuilder.repository.getDimension(dimensionPath);
          const { sql, bindings } = dimension.getSql(
            queryBuilder.repository,
            queryBuilder.dialect,
            context,
          );
          acc.sqls.push(
            `${sql} = ${queryBuilder.dialect.asIdentifier(METRIC_REF_SUBQUERY_ALIAS)}.${queryBuilder.dialect.asIdentifier(dimension.getAlias())}`,
          );
          acc.bindings.push(...bindings);
          return acc;
        },
        { sqls: [], bindings: [] },
      );

      sqlQuery.leftJoin(
        new SqlFragment(`(${sql}) as ${METRIC_REF_SUBQUERY_ALIAS}`, bindings),
        new SqlFragment(joinOn.sqls.join(" and "), joinOn.bindings),
      );
    }
  }

  return sqlQuery;
}

function buildSegmentQuery(
  queryBuilder: AnyQueryBuilder,
  context: unknown,
  segment: QueryPlan["segments"][number],
  alias?: string,
): SqlQueryBuilder {
  const modelQueryAlias = alias ?? segment.alias;

  const initialSqlQuery = buildModelQuery(queryBuilder, context, segment);

  if (segment.filters) {
    const filter = queryBuilder
      .getFilterBuilder()
      .buildFilters(segment.filters, "and", context);

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

  for (const memberName of segment.segmentQuery.members) {
    const member = queryBuilder.repository.getMember(memberName);
    const segmentQueryProjection = member.getSegmentQueryProjection(
      queryBuilder.repository,
      queryBuilder.dialect,
      context,
      modelQueryAlias,
    );

    for (const fragment of segmentQueryProjection) {
      sqlQuery.select(fragment);
    }

    // We always GROUP BY the dimensions, if there are no metrics, it will behave as DISTINCT
    // For metrics, this is currently NOOP because Metric returns an empty array
    const segmentQueryGroupBy = member.getSegmentQueryGroupBy(
      queryBuilder.repository,
      queryBuilder.dialect,
      context,
      modelQueryAlias,
    );

    for (const fragment of segmentQueryGroupBy) {
      sqlQuery.groupBy(fragment);
    }
  }

  return sqlQuery;
}

function buildRootQuery(
  queryBuilder: AnyQueryBuilder,
  context: unknown,
  queryPlan: QueryPlan,
): SqlQueryBuilder {
  const segments = queryPlan.segments;

  invariant(arrayHasAtLeastOneElement(segments), "No query segments found");

  if (segments.length === 1) {
    const sqlQuery = buildSegmentQuery(
      queryBuilder,
      context,
      segments[0]!,
      getAlias(0),
    );

    return sqlQuery;
  }

  const segmentsWithSqlQuery = segments.map((segment) => ({
    segment,
    sqlQuery: buildSegmentQuery(queryBuilder, context, segment),
  }));

  invariant(
    arrayHasAtLeastOneElement(segmentsWithSqlQuery),
    "No segments with sql query found",
  );

  const [initialSegmentWithSqlQuery, ...restSegmentsWithSqlQuery] =
    segmentsWithSqlQuery;

  const joinOnDimensions = queryPlan.projectedDimensions.map(
    (dimensionName) => {
      return queryBuilder.repository.getDimension(dimensionName);
    },
  );

  const rootQueryAlias = getAlias(0);
  const rootSqlQuery = queryBuilder.dialect.from(
    initialSegmentWithSqlQuery.sqlQuery.as(rootQueryAlias),
  );

  for (const memberName of initialSegmentWithSqlQuery.segment.rootQuery
    .members) {
    const member = queryBuilder.repository.getMember(memberName);
    const rootQueryProjection = member.getRootQueryProjection(
      queryBuilder.repository,
      queryBuilder.dialect,
      context,
      rootQueryAlias,
    );

    for (const fragment of rootQueryProjection) {
      rootSqlQuery.select(fragment);
    }
  }

  for (let i = 0; i < restSegmentsWithSqlQuery.length; i++) {
    const segmentWithSqlQuery = restSegmentsWithSqlQuery[i]!;
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
      segmentWithSqlQuery.sqlQuery.as(segmentQueryAlias),
      queryBuilder.dialect.fragment(joinOn),
    );

    for (const metricName of segmentWithSqlQuery.segment.rootQuery.metrics) {
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
  return rootSqlQuery;
}

export function buildQuery(
  queryBuilder: AnyQueryBuilder,
  context: unknown,
  queryPlan: QueryPlan,
) {
  const rootQuery = buildRootQuery(queryBuilder, context, queryPlan);

  if (queryPlan.filters) {
    const filter = queryBuilder
      .getFilterBuilder()
      .buildFilters(queryPlan.filters, "and", context);

    if (filter) {
      rootQuery.where(
        queryBuilder.dialect.fragment(filter.sql, filter.bindings),
      );
    }
  }

  const orderBy = (
    queryPlan.order || getDefaultOrderBy(queryBuilder.repository, queryPlan)
  ).map(({ member, direction }) => {
    const quotedMemberAlias = queryBuilder.dialect.asIdentifier(
      queryBuilder.repository.getMember(member).getAlias(),
    );
    return `${quotedMemberAlias} ${direction}`;
  });

  if (orderBy.length > 0) {
    rootQuery.orderBy(orderBy.join(", "));
  }

  if (queryPlan.limit) {
    rootQuery.limit(queryPlan.limit);
  }
  if (queryPlan.offset) {
    rootQuery.offset(queryPlan.offset);
  }

  return rootQuery;
}
