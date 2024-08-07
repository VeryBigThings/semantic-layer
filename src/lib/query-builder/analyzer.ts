import { AnyInputQuery, HierarchyConfig, HierarchyType } from "../types.js";

import { AnyQueryBuilder } from "../query-builder.js";
import { HierarchyElementConfig } from "../hierarchy.js";
import { compareBy } from "compare-by";
import invariant from "tiny-invariant";

/* import { compareBy } from 'compare-by';
import invariant from 'tiny-invariant';

const granularityComparator = compareBy([
  {
    key: 'idx',
    dir: 'asc',
  },
  {
    key: 'firstDimensionIdx',
    dir: 'asc',
  },
]);

export function getQueryGranularities(
  repository: semanticLayer.AnyRepository,
  query: semanticLayer.AnyInputQuery,
) {
  const repositoryGranularities = repository.granularities;
  const seenGranularities = repositoryGranularities
    .map((v, i) => [i, v] as const)
    .reduce<
      Record<
        'custom' | 'temporal',
        {
          idx: number;
          firstDimensionIdx: number;
          name: string;
          members: string[];
        }[]
      >
    >(
      (acc, [idx, granularity]) => {
        let firstDimensionIdx: null | number = null;
        const members: string[] = [];

        for (let i = 0; i < granularity.elements.length; i++) {
          const element = granularity.elements[i];

          if (typeof element === 'string') {
            if (query.members.includes(element)) {
              if (firstDimensionIdx === null) {
                firstDimensionIdx = i;
              }
              members.push(element);
            }
          } else {
            const elementElements = element.elements;
            for (const elementElement of elementElements) {
              if (query.members.includes(elementElement)) {
                if (firstDimensionIdx === null) {
                  firstDimensionIdx = i;
                }
                members.push(elementElement);
              }
            }
          }
        }
        if (firstDimensionIdx !== null) {
          acc[granularity.type].push({
            idx,
            firstDimensionIdx,
            name: granularity.name,
            members,
          });
        }
        return acc;
      },
      { custom: [], temporal: [] },
    );

  return {
    custom: seenGranularities.custom
      .sort(granularityComparator)
      .map(({ name, members }) => ({
        members,
        name,
      })),
    temporal: seenGranularities.temporal
      .sort(granularityComparator)
      .map(({ name, members }) => ({
        members,
        name,
      })),
  };
}

export function getQueriesForGranularity(
  repository: semanticLayer.AnyRepository,
  query: semanticLayer.AnyInputQuery,
  queryGranularity: ReturnType<typeof getQueryGranularities>['custom'][number],
  temporalGranularities: ReturnType<typeof getQueryGranularities>['temporal'],
) {
  console.log('GET QUERIES FOR GRANULARITY', queryGranularity);
  const granularity = repository.granularities.find(
    (g) => g.name === queryGranularity.name,
  );
  if (!granularity) {
    return null;
  }
  const granularityLevels = granularity.elements.reduce<{
    granularityAllDimensions: string[];
    levelsDimensions: string[][];
  }>(
    (acc, m) => {
      if (typeof m === 'string') {
        acc.granularityAllDimensions.push(m);
        acc.levelsDimensions.push([m]);
      } else {
        const elements = m.elements;
        acc.levelsDimensions.push(elements);
        for (const element of elements) {
          acc.granularityAllDimensions.push(element);
        }
      }
      return acc;
    },
    { granularityAllDimensions: [], levelsDimensions: [] },
  );

  const dimensions: string[] = [];
  const metrics: string[] = [];
  for (const queryMember of query.members) {
    if (granularityLevels.granularityAllDimensions.includes(queryMember)) {
      continue;
    } else {
      if (repository.getMember(queryMember).isMetric()) {
        metrics.push(queryMember);
      } else {
        dimensions.push(queryMember);
      }
    }
  }

  const sortedDimensions = dimensions.sort();
  const sortedMetrics = metrics.sort();

  const queriesInfo: {
    level: number;
    isFirstLevel: boolean;
    isLastLevel: boolean;
    baseQuery: semanticLayer.AnyInputQuery;
    granularityDimensions: string[];
    prevLevelsGranularityDimensions: string[];
  }[] = [];

  for (let i = 0; i < granularityLevels.levelsDimensions.length; i++) {
    const granularityDimensions = granularityLevels.levelsDimensions[i];
    const isFirstLevel = i === 0;

    const baseQuery = {
      ...query,
      members: [],
    };

    const prevLevelsGranularityDimensions = [
      ...(queriesInfo[i - 1]?.prevLevelsGranularityDimensions ?? []),
      ...(queriesInfo[i - 1]?.granularityDimensions ?? []),
    ];

    queriesInfo.push({
      level: i,
      isFirstLevel,
      isLastLevel: false,
      baseQuery,
      granularityDimensions,
      prevLevelsGranularityDimensions,
    });
  }

  queriesInfo.push({
    ...queriesInfo[queriesInfo.length - 1],
    isLastLevel: true,
    level: queriesInfo.length,
  });

  return {
    granularity,
    allGranularityDimensions: granularityLevels.granularityAllDimensions,
    dimensions: sortedDimensions,
    metrics: sortedMetrics,
    temporalDimensions: temporalGranularities.flatMap((g) => g.members),
    queriesInfo,
  };
}

export function getQueriesForSelectedGranularity(
  repository: semanticLayer.AnyRepository,
  query: semanticLayer.AnyInputQuery,
  queryGranularities: ReturnType<typeof getQueryGranularities>,
  granularityName?: string,
) {
  const queryGranularity = granularityName
    ? queryGranularities.custom.find((g) => g.name === granularityName)
    : queryGranularities.custom[0];

  if (!queryGranularity) {
    return null;
  }

  const queriesForGranularity = getQueriesForGranularity(
    repository,
    query,
    queryGranularity,
    queryGranularities.temporal,
  );

  return queriesForGranularity;
}

export function getQueryForGranularityLevel(
  queriesForGranularity: NonNullable<
    ReturnType<typeof getQueriesForGranularity>
  >,
  level: number = 0,
) {
  const queryInfoForLevel = queriesForGranularity.queriesInfo[level];

  invariant(queryInfoForLevel, `Query info for level ${level} not found`);

  return {
    ...queryInfoForLevel.baseQuery,
    members: [
      ...queryInfoForLevel.prevLevelsGranularityDimensions,
      ...queryInfoForLevel.granularityDimensions,
      ...queriesForGranularity.temporalDimensions,
      ...(queryInfoForLevel.isLastLevel
        ? queriesForGranularity.dimensions
        : []),
      ...queriesForGranularity.metrics,
    ],
  };
}*/

const hierarchyOrderComparator = compareBy([
  {
    key: "level",
    dir: "asc",
  },
  {
    key: "hierarchyOrderIndex",
    dir: "asc",
  },
]);

function getQueryHierarchies(
  queryBuilder: AnyQueryBuilder,
  query: AnyInputQuery,
) {
  const seenHierarchies = queryBuilder.hierarchies.reduce<
    { level: number; hierarchyOrderIndex: number; hierarchy: HierarchyConfig }[]
  >((acc, hierarchy, idx) => {
    for (let i = 0; i < hierarchy.elements.length; i++) {
      const element = hierarchy.elements[i]!;
      if (element.dimensions.some((value) => query.members.includes(value))) {
        acc.push({
          level: i,
          hierarchyOrderIndex: idx,
          hierarchy,
        });
        return acc;
      }
    }
    return acc;
  }, []);

  const result = seenHierarchies
    .sort(hierarchyOrderComparator)
    .reduce<Record<HierarchyType | "all", HierarchyConfig[]>>(
      (acc, { hierarchy }) => {
        acc.all.push(hierarchy);
        acc[hierarchy.type].push(hierarchy);
        return acc;
      },
      { categorical: [], temporal: [], all: [] },
    );

  return result;
}

function getQueryDimensionsAndMetrics(
  queryBuilder: AnyQueryBuilder,
  query: AnyInputQuery,
) {
  return query.members.reduce<{
    dimensions: string[];
    metrics: string[];
  }>(
    (acc, memberName) => {
      const member = queryBuilder.repository.getMember(memberName);
      if (member.isDimension()) {
        acc.dimensions.push(memberName);
      } else {
        acc.metrics.push(memberName);
      }

      return acc;
    },
    { dimensions: [], metrics: [] },
  );
}

export function analyzeQuery(
  queryBuilder: AnyQueryBuilder,
  query: AnyInputQuery,
) {
  const hierarchies = getQueryHierarchies(queryBuilder, query);
  const { dimensions, metrics } = getQueryDimensionsAndMetrics(
    queryBuilder,
    query,
  );
  return {
    query,
    dimensions,
    metrics,
    hierarchies,
  };
}

export type QueryAnalysis = ReturnType<typeof analyzeQuery>;

export function getQueriesForHierarchy(
  analysis: QueryAnalysis,
  hierarchyName: string,
) {
  const hierarchy = analysis.hierarchies.all.find(
    (h) => h.name === hierarchyName,
  );
  invariant(hierarchy, `Hierarchy ${hierarchyName} not found`);

  const queriesForHierarchy = hierarchy.elements.reduce<{
    elementsExtraDimensions: string[];
    restDimensions: string[];
    queriesInfo: {
      hierarchyElement: HierarchyElementConfig;
      keyDimensions: string[];
      prevLevelsKeyDimensions: string[];
      formatDimensions: string[];
      extraDimensions: string[];
      prevLevelsExtraDimensions: string[];
    }[];
  }>(
    (acc, element, idx) => {
      const selectedElementDimensions = element.dimensions.filter((dimension) =>
        analysis.dimensions.includes(dimension),
      );

      // Dimensions that are not key dimensions and not format dimensions
      const elementExtraDimensions = selectedElementDimensions.filter(
        (dimension) =>
          !(
            element.keyDimensions.includes(dimension) ||
            element.formatDimensions.includes(dimension)
          ),
      );

      // First remove all element dimensions from the selected dimensions list
      acc.restDimensions = acc.restDimensions.filter(
        (dimension) => !selectedElementDimensions.includes(dimension),
      );

      // If key dimension was selected AND it's not a part of format dimensions, add it to the rest dimensions so it will be displayed in the result
      for (const dimension of element.keyDimensions) {
        if (
          !element.formatDimensions.includes(dimension) &&
          selectedElementDimensions.includes(dimension)
        ) {
          acc.restDimensions.push(dimension);
        }
      }

      // Add all non-key non-format dimensions to the elements extra dimensions. These will be prepended to the rest dimensions and displayed first in the result
      acc.elementsExtraDimensions.push(...elementExtraDimensions);

      const queryInfo = {
        hierarchyElement: element,
        keyDimensions: element.keyDimensions,
        formatDimensions: element.formatDimensions,
        extraDimensions: elementExtraDimensions,
        prevLevelsExtraDimensions: [
          ...(acc.queriesInfo[idx - 1]?.prevLevelsExtraDimensions ?? []),
          ...(acc.queriesInfo[idx - 1]?.extraDimensions ?? []),
        ],
        prevLevelsKeyDimensions: [
          ...(acc.queriesInfo[idx - 1]?.prevLevelsKeyDimensions ?? []),
          ...(acc.queriesInfo[idx - 1]?.keyDimensions ?? []),
        ],
      };

      acc.queriesInfo.push(queryInfo);

      return acc;
    },
    {
      elementsExtraDimensions: [],
      restDimensions: [...analysis.dimensions],
      queriesInfo: [],
    },
  );

  return {
    restMembers: [
      ...queriesForHierarchy.elementsExtraDimensions,
      ...queriesForHierarchy.restDimensions,
      ...analysis.metrics,
    ],
    // Repeat the last query because we will add all the rest dimensions to it
    queriesInfo: [
      ...queriesForHierarchy.queriesInfo,
      queriesForHierarchy.queriesInfo[
        queriesForHierarchy.queriesInfo.length - 1
      ]!,
    ].map((queryInfo, idx) => ({
      hierarchyElement: queryInfo.hierarchyElement,
      hierarchyElementFilterDimensions: queryInfo.prevLevelsKeyDimensions,
      query: {
        ...analysis.query,
        members: [
          ...queryInfo.prevLevelsKeyDimensions,
          ...queryInfo.keyDimensions,
          ...queryInfo.prevLevelsExtraDimensions,
          ...queryInfo.extraDimensions,
          // We check for the length instead of length - 1 because we've duplicated the last query to add all the rest dimensions
          ...(idx === queriesForHierarchy.queriesInfo.length
            ? queriesForHierarchy.restDimensions
            : []),
          ...analysis.metrics,
        ],
      },
    })),
  };
}
