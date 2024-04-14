import {
  AnyQuery,
  AnyQueryFilter,
  ModelQuery,
  QuerySegment,
} from "../types.js";

import { AnyRepository } from "../repository.js";

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: <explanation>
function analyzeQuery(repository: AnyRepository, query: AnyQuery) {
  const allModels = new Set<string>();
  const dimensionModels = new Set<string>();
  const metricModels = new Set<string>();
  const projectedDimensionsByModel: Record<string, Set<string>> = {};
  const dimensionsByModel: Record<string, Set<string>> = {};
  const projectedMetricsByModel: Record<string, Set<string>> = {};
  const metricsByModel: Record<string, Set<string>> = {};
  const allMemberNames = new Set<string>();

  for (const dimension of query.dimensions || []) {
    const modelName = repository.getDimension(dimension).model.name;
    allModels.add(modelName);
    allMemberNames.add(dimension);
    dimensionModels.add(modelName);
    dimensionsByModel[modelName] ||= new Set<string>();
    dimensionsByModel[modelName]!.add(dimension);
    projectedDimensionsByModel[modelName] ||= new Set<string>();
    projectedDimensionsByModel[modelName]!.add(dimension);
  }

  for (const metric of query.metrics || []) {
    const modelName = repository.getMetric(metric).model.name;
    allModels.add(modelName);
    allMemberNames.add(metric);
    metricModels.add(modelName);
    metricsByModel[modelName] ||= new Set<string>();
    metricsByModel[modelName]!.add(metric);
    projectedMetricsByModel[modelName] ||= new Set<string>();
    projectedMetricsByModel[modelName]!.add(metric);
  }

  const filterStack: AnyQueryFilter[] = [...(query.filters || [])];

  while (filterStack.length > 0) {
    const filter = filterStack.pop()!;
    if (filter.operator === "and" || filter.operator === "or") {
      filterStack.push(...filter.filters);
    } else {
      const member = repository.getMember(filter.member);
      const modelName = member.model.name;

      allModels.add(modelName);
      allMemberNames.add(filter.member);

      if (member.isDimension()) {
        // dimensionModels are used for join of query segments
        // so we're not adding them here, because we don't have
        // a guarantee that join on dimensions will be projected
        // (and if we projected them automatically, we'd get wrong results)
        // In the segment query allModels are used to join models, which
        // means that any dimension filters will work
        dimensionsByModel[modelName] ||= new Set<string>();
        dimensionsByModel[modelName]!.add(filter.member);
      } else {
        metricModels.add(modelName);
        metricsByModel[modelName] ||= new Set<string>();
        metricsByModel[modelName]!.add(filter.member);
      }
    }
  }

  const orderByWithoutNonProjectedMembers = Object.entries(
    query.order ?? {},
  ).reduce<Record<string, "asc" | "desc">>((acc, [member, direction]) => {
    if (allMemberNames.has(member)) {
      acc[member] = direction ?? "asc";
    }
    return acc;
  }, {});

  return {
    allModels,
    dimensionModels,
    metricModels,
    dimensionsByModel,
    projectedDimensionsByModel,
    metricsByModel,
    projectedMetricsByModel,
    order:
      Object.keys(orderByWithoutNonProjectedMembers).length > 0
        ? orderByWithoutNonProjectedMembers
        : undefined,
  };
}

interface PreparedQuery {
  dimensions: Set<string>;
  metrics: Set<string>;
  filters: [];
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: <explanation>
function getQuerySegment(
  queryAnalysis: ReturnType<typeof analyzeQuery>,
  metricModel: string | null,
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

  const referencedModels = {
    all: new Set<string>(queryAnalysis.allModels),
    dimensions: new Set<string>(),
    metrics: new Set<string>(),
  };

  const modelQueries: Record<string, ModelQuery> = {};

  for (const q of queriesKeys) {
    for (const [modelName, dimensions] of Object.entries(
      queryAnalysis.projectedDimensionsByModel,
    )) {
      referencedModels.all.add(modelName);
      referencedModels.dimensions.add(modelName);

      for (const dimension of dimensions) {
        queries[q].dimensions.add(dimension);
      }

      modelQueries[modelName] = {
        dimensions: new Set<string>(dimensions),
        metrics: new Set<string>(),
      };
    }
  }

  if (metricModel) {
    referencedModels.all.add(metricModel);
    referencedModels.metrics.add(metricModel);
    modelQueries[metricModel] ||= {
      dimensions: new Set<string>(),
      metrics: new Set<string>(),
    };

    for (const q of queriesKeys) {
      const metrics = metricModel
        ? queryAnalysis[
            q === "query" ? "metricsByModel" : "projectedMetricsByModel"
          ][metricModel] ?? new Set<string>()
        : new Set<string>();
      for (const metric of metrics) {
        queries[q].metrics.add(metric);
        modelQueries[metricModel]!.metrics.add(metric);
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
    referencedModels: {
      all: Array.from(referencedModels.all),
      dimensions: Array.from(referencedModels.dimensions),
      metrics: Array.from(referencedModels.metrics),
    },
    modelQueries: modelQueries,
    metricModel: metricModel,
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

export function processQueryAndExpandToSegments(
  repository: AnyRepository,
  query: AnyQuery,
) {
  const queryAnalysis = analyzeQuery(repository, query);
  const metricModels = Object.keys(queryAnalysis.metricsByModel);
  const segments =
    metricModels.length === 0
      ? [
          mergeQuerySegmentWithFilters(
            getQuerySegment(queryAnalysis, null),
            query.filters,
          ),
        ]
      : metricModels.map((model) =>
          mergeQuerySegmentWithFilters(
            getQuerySegment(queryAnalysis, model),
            query.filters,
          ),
        );

  return {
    query: { ...query, order: queryAnalysis.order },
    referencedModels: {
      all: Array.from(queryAnalysis.allModels),
      dimensions: Array.from(queryAnalysis.dimensionModels),
      metrics: Array.from(queryAnalysis.metricModels),
    },
    segments,
  };
}
