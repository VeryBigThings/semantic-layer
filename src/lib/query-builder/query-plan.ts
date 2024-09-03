import { Dimension, Member, Metric } from "../member.js";
import { AnyModel, AnyQueryBuilder } from "../semantic-layer.js";
import { AnyInputQuery, Order } from "../types.js";

import { AnyJoin } from "../join.js";
import { BasicMetricQueryMember } from "../model/basic-metric.js";
import { AnyRepository } from "../repository.js";
import { CalculatedMetricQueryMember } from "../repository/calculated-metric.js";
import { findOptimalJoinGraph } from "./optimal-join-graph.js";
import { QueryContext } from "./query-plan/query-context.js";

export type QueryPlanQueryFilterConnective = {
  operator: "and" | "or";
  filters: QueryPlanQueryFilter[];
};

export type QueryPlanQueryFilter =
  | QueryPlanQueryFilterConnective
  | {
      operator: string;
      member: string;
      value: any;
    };

function filterIsConnective(
  filter: QueryPlanQueryFilter,
): filter is QueryPlanQueryFilterConnective {
  return filter.operator === "and" || filter.operator === "or";
}

const METRICS_WITHOUT_REFERENCED_MODELS_KEY =
  "__METRICS_WITHOUT_REFERENCED_MODELS_KEY__";

function getMetricSegmentKey(referencedModels: string[]) {
  return referencedModels.length > 0
    ? referencedModels.join("___")
    : METRICS_WITHOUT_REFERENCED_MODELS_KEY;
}

function getMetricSegments(
  queryContext: QueryContext,
  projectedMetrics: Metric[],
  filtersMetrics: Metric[],
) {
  const metricsByModel: Record<
    string | symbol,
    { projected: Metric[]; filter: Metric[]; referencedModels: Set<string> }
  > = {};

  for (const m of projectedMetrics) {
    const metricQueryMember = queryContext.getQueryMember(m);
    const referencedModels = metricQueryMember.getReferencedModels();
    const key = getMetricSegmentKey(referencedModels);

    metricsByModel[key] ||= {
      projected: [],
      filter: [],
      referencedModels: new Set(),
    };
    metricsByModel[key]!.projected.push(m);

    for (const modelName of referencedModels) {
      metricsByModel[key]!.referencedModels.add(modelName);
    }
  }

  for (const m of filtersMetrics) {
    const metricQueryMember = queryContext.getQueryMember(m);
    const referencedModels = metricQueryMember.getReferencedModels();
    const key = getMetricSegmentKey(referencedModels);

    metricsByModel[key] ||= {
      projected: [],
      filter: [],
      referencedModels: new Set(),
    };
    metricsByModel[key]!.filter.push(m);

    for (const modelName of referencedModels) {
      metricsByModel[key]!.referencedModels.add(modelName);
    }
  }

  return Object.values(metricsByModel).map((value) => {
    const referencedModels = Array.from(value.referencedModels);

    return {
      ...value,
      referencedModels:
        referencedModels.length > 0 ? referencedModels : undefined,
    };
  });
}

function getOrderWithOnlyProjectedMembers(
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

function getFirstMemberFilter(filter: QueryPlanQueryFilter) {
  if (filterIsConnective(filter)) {
    return getFirstMemberFilter(filter.filters[0]!);
  }
  return filter;
}

function getDimensionAndMetricFilters(
  repository: AnyRepository,
  filters: QueryPlanQueryFilter[] | undefined,
) {
  return (filters ?? []).reduce<{
    dimensionFilters: QueryPlanQueryFilter[];
    metricFilters: QueryPlanQueryFilter[];
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

function getFiltersMembers(
  repository: AnyRepository,
  filters: QueryPlanQueryFilter[],
) {
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

const MEMBER_SETS = ["projected", "filter"] as const;

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: This needs to be rewritten so we compute dimension data only once and then use it for all metrics
function getSegmentQueryModelsAndMembers(
  queryBuilder: AnyQueryBuilder,
  queryContext: QueryContext,
  { dimensions, metrics }: Omit<SegmentInput, "filters">,
) {
  const models = new Set<AnyModel>();
  const modelQueryDimensions = new Set<Dimension>();
  const modelQueryMetrics = new Set<Metric>();
  const segmentQueryDimensions = new Set<Dimension>();
  const segmentQueryMetrics = new Set<Metric>();
  const rootQueryDimensions = new Set<Dimension>();
  const rootQueryMetrics = new Set<Metric>();

  for (const memberSet of MEMBER_SETS) {
    for (const dimension of dimensions[memberSet]) {
      // If we are dealing with a projected dimension, we need to add it to the all query levels (model, segment, root). If we are dealing with a filter dimension, we only need to add it to the model query level because we don't care about it's value in the segment or root query.

      if (memberSet === "projected") {
        modelQueryDimensions.add(dimension);
        segmentQueryDimensions.add(dimension);
        rootQueryDimensions.add(dimension);
      } else if (memberSet === "filter") {
        modelQueryDimensions.add(dimension);
      }

      const dimensionQueryMember = queryContext.getQueryMember(dimension);
      const referencedModels = dimensionQueryMember.getReferencedModels();

      for (const modelName of referencedModels) {
        models.add(queryBuilder.repository.getModel(modelName));
      }
    }
  }

  if (metrics) {
    for (const memberSet of MEMBER_SETS) {
      for (const metric of metrics[memberSet]) {
        // If we are dealing with a projected metric, we need to add it to the all query levels (model, segment, root). If we are dealing with a filter metric, we need to add it to the model and segment query levels, so we can filter the data in the root query (but without projecting the value)
        if (memberSet === "projected") {
          modelQueryMetrics.add(metric);
          segmentQueryMetrics.add(metric);
          rootQueryMetrics.add(metric);
        } else if (memberSet === "filter") {
          modelQueryMetrics.add(metric);
          segmentQueryMetrics.add(metric);
        }

        for (const modelName of metrics.referencedModels ?? []) {
          const metricModel = queryBuilder.repository.getModel(modelName);
          models.add(metricModel);

          const primaryKeyDimensions = metricModel.getPrimaryKeyDimensions();
          for (const primaryKeyDimension of primaryKeyDimensions) {
            modelQueryDimensions.add(primaryKeyDimension);
          }
          models.add(metricModel);
        }
      }
    }
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
  queryContext: QueryContext,
  context: unknown,
  dimensions: string[],
  metrics: string[],
  filters: QueryPlanQueryFilter[],
): { joinOnDimensions: string[]; queryPlan: QueryPlan } | undefined {
  const metricRefs = Array.from(
    new Set(
      metrics.flatMap((metricPath) => {
        const metricQueryMember = queryContext.getQueryMemberByPath(metricPath);
        if (
          metricQueryMember instanceof BasicMetricQueryMember ||
          metricQueryMember instanceof CalculatedMetricQueryMember
        ) {
          return metricQueryMember
            .getMetricRefs()
            .map((metricRef) => metricRef.member.getPath());
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
    const queryPlan = getQueryPlan(queryBuilder, queryContext, context, query);
    const joinOnDimensions = [...dimensions];
    return {
      queryPlan,
      joinOnDimensions,
    };
  }
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Essential complexity for join planning
function getSegmentQueryJoins(
  queryBuilder: AnyQueryBuilder,
  models: string[],
  initialModel: string,
) {
  const joinGraph = findOptimalJoinGraph(queryBuilder.repository.graph, models);
  const visitedModels = new Set<string>();
  const modelsToProcess: {
    modelName: string;
    join?: { config: AnyJoin; leftModel: string; rightModel: string };
  }[] = [{ modelName: initialModel }];

  const joins: {
    leftModel: string;
    rightModel: string;
    joinType: "left" | "right" | "inner" | "full";
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
      if (
        join.config.type === "manyToMany" ||
        join.config.type === "oneToMany"
      ) {
        hasRowMultiplication = true;
      }
      const joinType = join.config.joinType
        ? join.config.joinType
        : join.config.reversed
          ? "right"
          : "left";

      joins.push({
        leftModel: join.leftModel,
        rightModel: join.rightModel,
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
            ? {
                leftModel: modelName,
                rightModel: unvisitedModelName,
                config: join,
              }
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

interface SegmentInput {
  dimensions: { projected: Dimension[]; filter: Dimension[] };
  metrics?: ReturnType<typeof getMetricSegments>[number];
  filters: QueryPlanQueryFilter[];
}

function getSegmentQuery(
  queryBuilder: AnyQueryBuilder,
  queryContext: QueryContext,
  context: unknown,
  alias: string,
  { dimensions, metrics, filters }: SegmentInput,
) {
  const segmentModelsAndMembers = getSegmentQueryModelsAndMembers(
    queryBuilder,
    queryContext,
    {
      dimensions,
      metrics,
    },
  );

  const initialModel =
    metrics?.referencedModels?.[0] ?? segmentModelsAndMembers.models[0];

  const joinPlan = initialModel
    ? getSegmentQueryJoins(
        queryBuilder,
        segmentModelsAndMembers.models,
        initialModel,
      )
    : undefined;

  return {
    ...segmentModelsAndMembers,
    metricsRefsSubQueryPlan: getSegmentQueryMetricsRefsSubQueryPlan(
      queryBuilder,
      queryContext,
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

export function getQueryPlan(
  queryBuilder: AnyQueryBuilder,
  queryContext: QueryContext,
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

  const metricSegments = getMetricSegments(
    queryContext,
    projectedMetrics,
    filtersMetrics,
  );

  const segments =
    metricSegments.length > 0
      ? metricSegments.map((metricSegment, index) =>
          getSegmentQuery(
            queryBuilder,
            queryContext,
            context,
            getSegmentAlias(index),
            {
              dimensions: {
                projected: projectedDimensions,
                filter: filtersDimensions,
              },
              metrics: metricSegment,
              filters: dimensionFilters,
            },
          ),
        )
      : [
          getSegmentQuery(
            queryBuilder,
            queryContext,
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
    order: getOrderWithOnlyProjectedMembers(query.order, [
      ...projectedDimensionPaths,
      ...projectedMetricPaths,
    ]),
  };
}

export type QueryPlan = ReturnType<typeof getQueryPlan>;
