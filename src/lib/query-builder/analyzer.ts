import {
  AnyInputQuery,
  HierarchyConfig,
  HierarchyType,
  Order as _Order,
} from "../types.js";

import { compareBy } from "compare-by";
import { HierarchyElementConfig } from "../hierarchy.js";
import { AnyQueryBuilder } from "../query-builder.js";

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
    .reduce<
      Record<
        HierarchyType | "all",
        { hierarchy: HierarchyConfig; level: number }[]
      >
    >(
      (acc, { hierarchy, level }) => {
        acc.all.push({ hierarchy, level });
        acc[hierarchy.type].push({
          hierarchy,
          level,
        });
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

// TODO: Change args to take in hierarchy instead of hierarchy name
export function analyzeQueryHierarchy(
  analysis: QueryAnalysis,
  hierarchy: HierarchyConfig,
) {
  const queriesForHierarchy = hierarchy.elements.reduce<{
    elementsExtraDimensions: string[];
    restDimensions: string[];
    queriesInfo: {
      element: HierarchyElementConfig;
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
        element: element,
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
    hierarchy,
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
      element: queryInfo.element,
      keyDimensions: [
        ...queryInfo.prevLevelsKeyDimensions,
        ...queryInfo.keyDimensions,
      ],
      query: {
        ...analysis.query,
        members: [
          ...new Set([
            ...queryInfo.prevLevelsKeyDimensions,
            ...queryInfo.keyDimensions,
            ...queryInfo.formatDimensions,
            ...queryInfo.prevLevelsExtraDimensions,
            ...queryInfo.extraDimensions,
            // We check for the length instead of length - 1 because we've duplicated the last query to add all the rest dimensions so the length of the array we're iterating over is one element longer thant the queriesForHierarchy.queriesInfo.length
            ...(idx === queriesForHierarchy.queriesInfo.length
              ? queriesForHierarchy.restDimensions
              : []),
            ...analysis.metrics,
          ]),
        ],
        order: [
          ...queryInfo.formatDimensions.map((dimension) => ({
            member: dimension,
            direction: "asc" as const,
          })),
          ...(analysis.query.order ?? []),
        ],
      },
    })),
  };
}

export type QueryHierarchyAnalysis = ReturnType<typeof analyzeQueryHierarchy>;
