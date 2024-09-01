import { SqlFragment, SqlQueryBuilder } from "../sql-builder.js";
import { METRIC_REF_SUBQUERY_ALIAS, isNonEmptyArray } from "../util.js";

import invariant from "tiny-invariant";
import { AnyQueryBuilder } from "../query-builder.js";
import type { AnyRepository } from "../repository.js";
import { Order } from "../types.js";
import { QueryPlan } from "./query-plan.js";
import { QueryContext } from "./query-plan/query-context.js";

function getAlias(index: number) {
  return `q${index}`;
}

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

function joinModelQueryModels(
  queryBuilder: AnyQueryBuilder,
  queryContext: QueryContext,
  context: unknown,
  segment: QueryPlan["segments"][number],
  sqlQuery: SqlQueryBuilder,
) {
  invariant(segment.joinPlan, "Join plan not found");
  if (segment.joinPlan.hasRowMultiplication) {
    sqlQuery.distinct();
  }
  for (const {
    leftModel: leftModelName,
    rightModel: rightModelName,
    joinType,
  } of segment.joinPlan.joins) {
    const join = queryBuilder.repository.getJoin(leftModelName, rightModelName);

    invariant(
      join,
      `Join not found between ${leftModelName} and ${rightModelName}`,
    );

    const joinOn = join
      .joinOnDef(context)
      .render(queryBuilder.repository, queryContext, queryBuilder.dialect);

    const rightModel = queryBuilder.repository.getModel(rightModelName);
    const joinSubject = rightModel.getTableNameOrSql(
      queryBuilder.repository,
      queryContext,
      queryBuilder.dialect,
      context,
    );

    sqlQuery[joinType](
      joinSubject,
      queryBuilder.dialect.fragment(joinOn.sql, joinOn.bindings),
    );
  }
}

function joinModelQueryMetricRefsSubQuery(
  queryBuilder: AnyQueryBuilder,
  queryContext: QueryContext,
  context: unknown,
  segment: QueryPlan["segments"][number],
  sqlQuery: SqlQueryBuilder,
) {
  if (segment.metricsRefsSubQueryPlan) {
    const { sql, bindings } = buildQuery(
      queryBuilder,
      queryContext,
      context,
      segment.metricsRefsSubQueryPlan.queryPlan,
    ).toSQL();

    const joinOn = segment.metricsRefsSubQueryPlan.joinOnDimensions.reduce<{
      sqls: string[];
      bindings: unknown[];
    }>(
      (acc, dimensionPath) => {
        const dimensionQueryMember =
          queryContext.getQueryMemberByPath(dimensionPath);

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
}

function buildModelQuery(
  queryBuilder: AnyQueryBuilder,
  queryContext: QueryContext,
  context: unknown,
  segment: QueryPlan["segments"][number],
) {
  if (segment.joinPlan) {
    const model = queryBuilder.repository.getModel(
      segment.joinPlan.initialModel,
    );
    const sqlQuery = queryBuilder.dialect.from(
      model.getTableNameOrSql(
        queryBuilder.repository,
        queryContext,
        queryBuilder.dialect,
        context,
      ),
    );

    for (const memberPath of segment.modelQuery.members) {
      const queryMember = queryContext.getQueryMemberByPath(memberPath);
      const modelQueryProjection = queryMember.getModelQueryProjection();

      for (const fragment of modelQueryProjection) {
        sqlQuery.select(fragment);
      }
    }

    joinModelQueryModels(
      queryBuilder,
      queryContext,
      context,
      segment,
      sqlQuery,
    );
    joinModelQueryMetricRefsSubQuery(
      queryBuilder,
      queryContext,
      context,
      segment,
      sqlQuery,
    );

    return sqlQuery;
  }
  if (segment.metricsRefsSubQueryPlan) {
    const metricRefsSubQuery = buildQuery(
      queryBuilder,
      queryContext,
      context,
      segment.metricsRefsSubQueryPlan.queryPlan,
    ).toSQL();
    const sqlQuery = queryBuilder.dialect.from(
      new SqlFragment(
        `(${metricRefsSubQuery.sql}) as ${METRIC_REF_SUBQUERY_ALIAS}`,
        metricRefsSubQuery.bindings,
      ),
    );
    for (const memberPath of segment.modelQuery.members) {
      const queryMember = queryContext.getQueryMemberByPath(memberPath);
      const modelQueryProjection = queryMember.getModelQueryProjection();

      for (const fragment of modelQueryProjection) {
        sqlQuery.select(fragment);
      }
    }
    return sqlQuery;
  }
  throw new Error(
    "Segment must either have a join plan or a metrics refs sub query plan",
  );
}

function buildSegmentQuery(
  queryBuilder: AnyQueryBuilder,
  queryContext: QueryContext,
  context: unknown,
  segment: QueryPlan["segments"][number],
  alias?: string,
): SqlQueryBuilder {
  const modelQueryAlias = alias ?? segment.alias;

  const initialSqlQuery = buildModelQuery(
    queryBuilder,
    queryContext,
    context,
    segment,
  );

  if (segment.filters) {
    const filter = queryBuilder
      .getFilterBuilder(queryContext)
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
    const queryMember = queryContext.getQueryMemberByPath(memberPath);
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
  queryContext: QueryContext,
  context: unknown,
  queryPlan: QueryPlan,
): SqlQueryBuilder {
  const segments = queryPlan.segments;

  invariant(isNonEmptyArray(segments), "No query segments found");

  if (segments.length === 1) {
    const sqlQuery = buildSegmentQuery(
      queryBuilder,
      queryContext,
      context,
      segments[0]!,
      getAlias(0),
    );

    return sqlQuery;
  }

  const segmentsWithSqlQuery = segments.map((segment) => ({
    segment,
    sqlQuery: buildSegmentQuery(queryBuilder, queryContext, context, segment),
  }));

  invariant(
    isNonEmptyArray(segmentsWithSqlQuery),
    "No segments query segments found",
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
    const queryMember = queryContext.getQueryMemberByPath(memberPath);
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
      const queryMember = queryContext.getQueryMemberByPath(metricPath);
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
  queryContext: QueryContext,
  context: unknown,
  queryPlan: QueryPlan,
) {
  const rootQuery = buildRootQuery(
    queryBuilder,
    queryContext,
    context,
    queryPlan,
  );

  if (queryPlan.filters) {
    const filter = queryBuilder
      .getFilterBuilder(queryContext)
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
