import { SqlFragment, SqlQueryBuilder } from "../sql-builder.js";

import invariant from "tiny-invariant";
import type { AnyJoin } from "../join.js";
import { AnyQueryBuilder } from "../query-builder.js";
import type { AnyRepository } from "../repository.js";
import { Order } from "../types.js";
import { METRIC_REF_SUBQUERY_ALIAS } from "../util.js";
import { QueryPlan } from "./query-plan.js";

function getDefaultOrderBy(
  repository: AnyRepository,
  query: QueryPlan,
): Order[] {
  const firstDimensionName = query.projectedDimensions?.[0];
  const firstMetricName = query.projectedMetrics?.[0];

  for (const dimensionName of query.projectedDimensions ?? []) {
    const dimension = repository.getDimension(dimensionName);
    if (dimension.isGranularity()) {
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
      segment.queryMembers,
      queryBuilder.dialect,
      context,
    ),
  );

  for (const memberPath of segment.modelQuery.members) {
    const queryMember = segment.queryMembers.getByPath(memberPath);
    const modelQueryProjection = queryMember.getModelQueryProjection();

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
        .render(
          queryBuilder.repository,
          segment.queryMembers,
          queryBuilder.dialect,
        );
      const rightModel = queryBuilder.repository.getModel(join.right);
      const joinSubject = rightModel.getTableNameOrSql(
        queryBuilder.repository,
        segment.queryMembers,
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
  }

  if (segment.metricsRefsSubQueryPlan) {
    const { sql, bindings } = buildQuery(
      queryBuilder,
      context,
      segment.metricsRefsSubQueryPlan.queryPlan,
    ).toSQL();

    const joinOn = segment.metricsRefsSubQueryPlan.joinOnDimensions.reduce<{
      sqls: string[];
      bindings: unknown[];
    }>(
      (acc, dimensionPath) => {
        const dimensionQueryMember =
          segment.queryMembers.getByPath(dimensionPath);

        const { sql, bindings } = dimensionQueryMember.getSql();
        acc.sqls.push(
          `${sql} = ${queryBuilder.dialect.asIdentifier(METRIC_REF_SUBQUERY_ALIAS)}.${queryBuilder.dialect.asIdentifier(dimensionQueryMember.getAlias())}`,
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
      .getFilterBuilder(segment.queryMembers)
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

  for (const memberPath of segment.segmentQuery.members) {
    const queryMember = segment.queryMembers.getByPath(memberPath);
    const segmentQueryProjection =
      queryMember.getSegmentQueryProjection(modelQueryAlias);

    for (const fragment of segmentQueryProjection) {
      sqlQuery.select(fragment);
    }

    // We always GROUP BY the dimensions, if there are no metrics, it will behave as DISTINCT
    // For metrics, this is currently NOOP because Metric returns an empty array
    const segmentQueryGroupBy =
      queryMember.getSegmentQueryGroupBy(modelQueryAlias);

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

  for (const memberPath of initialSegmentWithSqlQuery.segment.rootQuery
    .members) {
    const queryMember = queryPlan.queryMembers.getByPath(memberPath);
    const rootQueryProjection =
      queryMember.getRootQueryProjection(rootQueryAlias);

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

    for (const metricPath of segmentWithSqlQuery.segment.rootQuery.metrics) {
      const queryMember = queryPlan.queryMembers.getByPath(metricPath);
      const rootQueryProjection =
        queryMember.getRootQueryProjection(segmentQueryAlias);

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
      .getFilterBuilder(queryPlan.queryMembers)
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
