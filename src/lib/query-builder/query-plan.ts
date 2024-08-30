import { Dimension, Member, Metric } from "../member.js";
import { AnyModel, AnyQueryBuilder } from "../semantic-layer.js";
import { AnyInputQuery, Order } from "../types.js";

import invariant from "tiny-invariant";
import { AnyJoin } from "../join.js";
import { BasicMetricQueryMember } from "../model/basic-metric.js";
import { AnyRepository } from "../repository.js";
import { findOptimalJoinGraph } from "./optimal-join-graph.js";
import { QueryMemberCache } from "./query-plan/query-member.js";

export type QueryFilterConnective = {
  operator: "and" | "or";
  filters: QueryFilter[];
};

export type QueryFilter =
  | QueryFilterConnective
  | {
      operator: string;
      member: string;
      value: any;
    };

function filterIsConnective(
  filter: QueryFilter,
): filter is QueryFilterConnective {
  return filter.operator === "and" || filter.operator === "or";
}

function getFirstMemberFilter(filter: QueryFilter) {
  if (filterIsConnective(filter)) {
    return getFirstMemberFilter(filter.filters[0]!);
  }
  return filter;
}

function getDimensionAndMetricFilters(
  repository: AnyRepository,
  filters: QueryFilter[] | undefined,
) {
  return (filters ?? []).reduce<{
    dimensionFilters: QueryFilter[];
    metricFilters: QueryFilter[];
  }>(
    (acc, filter) => {
      const memberFilter = getFirstMemberFilter(filter);
      const member = repository.getMember(memberFilter.member);
      if (member.isDimension()) {
        acc.dimensionFilters.push(filter);
      } else {
        acc.metricFilters.push(filter);
      }
      return acc;
    },
    { dimensionFilters: [], metricFilters: [] },
  );
}

function getFiltersMembers(repository: AnyRepository, filters: QueryFilter[]) {
  const members: Member[] = [];
  const filtersToProcess = [...filters];
  while (filtersToProcess.length > 0) {
    const filter = filtersToProcess.pop()!;
    if (filterIsConnective(filter)) {
      filtersToProcess.push(...filter.filters);
    } else {
      const member = repository.getMember(filter.member);
      members.push(member);
    }
  }
  return members;
}

function getMembersDimensionsAndMetrics(
  repository: AnyRepository,
  members: string[],
) {
  return members.reduce<{
    dimensions: Dimension[];
    metrics: Metric[];
  }>(
    (acc, memberName) => {
      const member = repository.getMember(memberName);
      if (member.isDimension()) {
        acc.dimensions.push(member);
      } else {
        acc.metrics.push(member);
      }
      return acc;
    },
    { dimensions: [], metrics: [] },
  );
}

function getSegmentAlias(index: number) {
  return `s${index}`;
}

function getSegmentQueryModelsAndMembers({
  dimensions,
  metrics,
}: {
  dimensions: { projected: Dimension[]; filter: Dimension[] };
  metrics?: {
    projected: Metric[];
    filter: Metric[];
    model: string;
  };
}) {
  const models = new Set<AnyModel>();
  const modelQueryDimensions = new Set<Dimension>();
  const modelQueryMetrics = new Set<Metric>();
  const segmentQueryDimensions = new Set<Dimension>();
  const segmentQueryMetrics = new Set<Metric>();
  const rootQueryDimensions = new Set<Dimension>();
  const rootQueryMetrics = new Set<Metric>();

  for (const dimension of dimensions.projected) {
    modelQueryDimensions.add(dimension);
    segmentQueryDimensions.add(dimension);
    rootQueryDimensions.add(dimension);
    models.add(dimension.model);
  }

  for (const dimension of dimensions.filter) {
    modelQueryDimensions.add(dimension);
    models.add(dimension.model);
  }

  for (const metric of metrics?.projected ?? []) {
    modelQueryMetrics.add(metric);
    segmentQueryMetrics.add(metric);
    rootQueryMetrics.add(metric);

    const metricModel = metric.model;
    const primaryKeyDimensions = metricModel.getPrimaryKeyDimensions();
    for (const primaryKeyDimension of primaryKeyDimensions) {
      modelQueryDimensions.add(primaryKeyDimension);
    }
    models.add(metricModel);
  }

  for (const metric of metrics?.filter ?? []) {
    modelQueryMetrics.add(metric);
    segmentQueryMetrics.add(metric);

    const metricModel = metric.model;
    const primaryKeyDimensions = metricModel.getPrimaryKeyDimensions();
    for (const primaryKeyDimension of primaryKeyDimensions) {
      modelQueryDimensions.add(primaryKeyDimension);
    }
    models.add(metricModel);
  }

  const modelQueryDimensionsArray = Array.from(modelQueryDimensions).map((d) =>
    d.getPath(),
  );
  const segmentQueryDimensionsArray = Array.from(segmentQueryDimensions).map(
    (d) => d.getPath(),
  );
  const rootQueryDimensionsArray = Array.from(rootQueryDimensions).map((d) =>
    d.getPath(),
  );
  const modelQueryMetricsArray = Array.from(modelQueryMetrics).map((m) =>
    m.getPath(),
  );
  const segmentQueryMetricsArray = Array.from(segmentQueryMetrics).map((m) =>
    m.getPath(),
  );
  const rootQueryMetricsArray = Array.from(rootQueryMetrics).map((m) =>
    m.getPath(),
  );

  return {
    models: Array.from(models).map((m) => m.name),
    modelQuery: {
      dimensions: modelQueryDimensionsArray,
      metrics: modelQueryMetricsArray,
      members: [...modelQueryDimensionsArray, ...modelQueryMetricsArray],
    },
    segmentQuery: {
      dimensions: segmentQueryDimensionsArray,
      metrics: segmentQueryMetricsArray,
      members: [...segmentQueryDimensionsArray, ...segmentQueryMetricsArray],
    },
    rootQuery: {
      dimensions: rootQueryDimensionsArray,
      metrics: rootQueryMetricsArray,
      members: [...rootQueryDimensionsArray, ...rootQueryMetricsArray],
    },
  };
}

function getSegmentQueryMetricsRefsSubQueryPlan(
  queryBuilder: AnyQueryBuilder,
  queryMembers: QueryMemberCache,
  context: unknown,
  dimensions: string[],
  metrics: string[],
  filters: QueryFilter[],
): { joinOnDimensions: string[]; queryPlan: QueryPlan } | undefined {
  const metricRefs = Array.from(
    new Set(
      metrics.flatMap((metricPath) => {
        const metricQueryMember = queryMembers.getByPath(metricPath);
        if (metricQueryMember instanceof BasicMetricQueryMember) {
          return metricQueryMember
            .getMetricRefs()
            .map((metricRef) => metricRef.metric.getPath());
        }
        return [];
      }),
    ),
  );
  if (metricRefs.length > 0) {
    const query: AnyInputQuery = {
      members: [...dimensions, ...metricRefs],
      filters,
    };
    const queryPlan = getQueryPlan(queryBuilder, queryMembers, context, query);
    const joinOnDimensions = [...dimensions];
    return {
      queryPlan,
      joinOnDimensions,
    };
  }
}

function getSegmentQueryJoins(
  queryBuilder: AnyQueryBuilder,
  models: string[],
  initialModel: string,
) {
  const joinGraph = findOptimalJoinGraph(queryBuilder.repository.graph, models);
  const visitedModels = new Set<string>();
  const modelsToProcess: {
    modelName: string;
    join?: { join: AnyJoin; left: string; right: string };
  }[] = [{ modelName: initialModel }];

  const joins: {
    leftModel: string;
    rightModel: string;
    joinType: "leftJoin" | "rightJoin";
  }[] = [];
  let hasRowMultiplication = false;

  while (modelsToProcess.length > 0) {
    const { modelName, join } = modelsToProcess.pop()!;

    if (visitedModels.has(modelName)) {
      continue;
    }
    visitedModels.add(modelName);

    const unvisitedNeighbors = (joinGraph.neighbors(modelName) ?? []).filter(
      (modelName) => !visitedModels.has(modelName),
    );

    if (join) {
      if (join.join.type === "manyToMany" || join.join.type === "oneToMany") {
        hasRowMultiplication = true;
      }
      const joinType = join.join.reversed ? "rightJoin" : "leftJoin";

      joins.push({
        leftModel: join.left,
        rightModel: join.right,
        joinType,
      });
    }

    modelsToProcess.push(
      ...unvisitedNeighbors.map((unvisitedModelName) => {
        const join = queryBuilder.repository.getJoin(
          modelName,
          unvisitedModelName,
        );
        return {
          modelName: unvisitedModelName,
          join: join
            ? { left: modelName, right: unvisitedModelName, join }
            : undefined,
        };
      }),
    );
  }
  return {
    hasRowMultiplication,
    initialModel,
    joins,
  };
}

function getSegmentQuery(
  queryBuilder: AnyQueryBuilder,
  queryMembers: QueryMemberCache,
  context: unknown,
  alias: string,
  {
    dimensions,
    metrics,
    filters,
  }: {
    dimensions: { projected: Dimension[]; filter: Dimension[] };
    metrics?: {
      projected: Metric[];
      filter: Metric[];
      model: string;
    };
    filters: QueryFilter[];
  },
) {
  const initialModel =
    metrics?.model ??
    dimensions.projected[0]?.model.name ??
    dimensions.filter[0]?.model.name;

  invariant(initialModel, "Initial model name not found");

  const segmentModelsAndMembers = getSegmentQueryModelsAndMembers({
    dimensions,
    metrics,
  });

  const joinPlan = getSegmentQueryJoins(
    queryBuilder,
    segmentModelsAndMembers.models,
    initialModel,
  );

  return {
    ...segmentModelsAndMembers,
    metricsRefsSubQueryPlan: getSegmentQueryMetricsRefsSubQueryPlan(
      queryBuilder,
      queryMembers,
      context,
      segmentModelsAndMembers.modelQuery.dimensions,
      segmentModelsAndMembers.modelQuery.metrics,
      filters,
    ),
    alias,
    joinPlan,
    filters,
  };
}

function orderWithOnlyProjectedMembers(
  order: Order[] | undefined,
  projectedMembers: string[],
) {
  if (!order) {
    return;
  }
  const newOrder = order.filter(({ member }) =>
    projectedMembers.includes(member),
  );
  if (newOrder.length > 0) {
    return newOrder;
  }
}

function getMetricsByModel(
  projectedMetrics: Metric[],
  filtersMetrics: Metric[],
) {
  const metricsByModel: Record<
    string,
    { projected: Metric[]; filter: Metric[] }
  > = {};

  for (const m of projectedMetrics) {
    metricsByModel[m.model.name] ||= { projected: [], filter: [] };
    metricsByModel[m.model.name]!.projected.push(m);
  }

  for (const m of filtersMetrics) {
    metricsByModel[m.model.name] ||= { projected: [], filter: [] };
    metricsByModel[m.model.name]!.filter.push(m);
  }

  return Object.entries(metricsByModel);
}

export function getQueryPlan(
  queryBuilder: AnyQueryBuilder,
  queryMembers: QueryMemberCache,
  context: unknown,
  query: AnyInputQuery,
) {
  const repository = queryBuilder.repository;
  const { dimensions: projectedDimensions, metrics: projectedMetrics } =
    getMembersDimensionsAndMetrics(repository, query.members);
  const { dimensionFilters, metricFilters } = getDimensionAndMetricFilters(
    repository,
    query.filters,
  );
  const filtersDimensions = (
    getFiltersMembers(repository, dimensionFilters) as Dimension[]
  ).filter((dimension) => !projectedDimensions.includes(dimension));
  const filtersMetrics = (
    getFiltersMembers(repository, metricFilters) as Metric[]
  ).filter((metric) => !projectedMetrics.includes(metric));

  const metricsByModel = getMetricsByModel(projectedMetrics, filtersMetrics);

  const segments =
    metricsByModel.length > 0
      ? metricsByModel.map(([modelName, metrics], index) =>
          getSegmentQuery(
            queryBuilder,
            queryMembers,
            context,
            getSegmentAlias(index),
            {
              dimensions: {
                projected: projectedDimensions,
                filter: filtersDimensions,
              },
              metrics: {
                projected: metrics.projected,
                filter: metrics.filter,
                model: modelName,
              },
              filters: dimensionFilters,
            },
          ),
        )
      : [
          getSegmentQuery(
            queryBuilder,
            queryMembers,
            context,
            getSegmentAlias(0),
            {
              dimensions: {
                projected: projectedDimensions,
                filter: filtersDimensions,
              },
              filters: dimensionFilters,
            },
          ),
        ];

  const projectedDimensionPaths = projectedDimensions.map((d) => d.getPath());
  const projectedMetricPaths = projectedMetrics.map((m) => m.getPath());

  return {
    segments,
    filters: metricFilters,
    projectedDimensions: projectedDimensionPaths,
    projectedMetrics: projectedMetricPaths,
    limit: query.limit,
    offset: query.offset,
    order: orderWithOnlyProjectedMembers(query.order, [
      ...projectedDimensionPaths,
      ...projectedMetricPaths,
    ]),
  };
}

export type QueryPlan = ReturnType<typeof getQueryPlan>;
