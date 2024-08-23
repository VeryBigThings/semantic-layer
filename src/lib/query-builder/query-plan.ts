import {
  AnyModel,
  BasicDimension,
  BasicMetric,
  Member,
} from "../semantic-layer.js";
import { AnyInputQuery, Order } from "../types.js";

import invariant from "tiny-invariant";
import { AnyRepository } from "../repository.js";
import { findOptimalJoinGraph } from "./optimal-join-graph.js";

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
    dimensions: BasicDimension[];
    metrics: BasicMetric[];
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
  dimensions: { projected: BasicDimension[]; filter: BasicDimension[] };
  metrics?: {
    projected: BasicMetric[];
    filter: BasicMetric[];
    model: string;
  };
}) {
  const models = new Set<AnyModel>();
  const modelQueryDimensions = new Set<BasicDimension>();
  const modelQueryMetrics = new Set<BasicMetric>();
  const segmentQueryDimensions = new Set<BasicDimension>();
  const segmentQueryMetrics = new Set<BasicMetric>();
  const rootQueryDimensions = new Set<BasicDimension>();
  const rootQueryMetrics = new Set<BasicMetric>();

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

function getSegmentQuery(
  repository: AnyRepository,
  {
    dimensions,
    metrics,
    filters,
  }: {
    dimensions: { projected: BasicDimension[]; filter: BasicDimension[] };
    metrics?: {
      projected: BasicMetric[];
      filter: BasicMetric[];
      model: string;
    };
    filters: QueryFilter[];
  },
  alias: string,
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

  const joinGraph = findOptimalJoinGraph(
    repository.graph,
    segmentModelsAndMembers.models,
  );

  return {
    ...segmentModelsAndMembers,
    alias,
    joinGraph,
    initialModel,
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
  projectedMetrics: BasicMetric[],
  filtersMetrics: BasicMetric[],
) {
  const metricsByModel: Record<
    string,
    { projected: BasicMetric[]; filter: BasicMetric[] }
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

export function getQueryPlan(repository: AnyRepository, query: AnyInputQuery) {
  const { dimensions: projectedDimensions, metrics: projectedMetrics } =
    getMembersDimensionsAndMetrics(repository, query.members);
  const { dimensionFilters, metricFilters } = getDimensionAndMetricFilters(
    repository,
    query.filters,
  );
  const filtersDimensions = (
    getFiltersMembers(repository, dimensionFilters) as BasicDimension[]
  ).filter((dimension) => !projectedDimensions.includes(dimension));
  const filtersMetrics = (
    getFiltersMembers(repository, metricFilters) as BasicMetric[]
  ).filter((metric) => !projectedMetrics.includes(metric));

  const metricsByModel = getMetricsByModel(projectedMetrics, filtersMetrics);

  const segments =
    metricsByModel.length > 0
      ? metricsByModel.map(([modelName, metrics], index) =>
          getSegmentQuery(
            repository,
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
            getSegmentAlias(index),
          ),
        )
      : [
          getSegmentQuery(
            repository,
            {
              dimensions: {
                projected: projectedDimensions,
                filter: filtersDimensions,
              },
              filters: dimensionFilters,
            },
            getSegmentAlias(0),
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
