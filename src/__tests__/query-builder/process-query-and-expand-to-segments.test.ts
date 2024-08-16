import * as semanticLayer from "../../index.js";
import * as fullRepository from "../full-repository.js";

import { assert, it } from "vitest";

import { processQueryAndExpandToSegments } from "../../lib/query-builder/process-query-and-expand-to-segments.js";

it("can process query and expand to segments", () => {
  const { queryBuilder } = fullRepository;
  const query: semanticLayer.Query = {
    dimensions: ["artists.name"],
    metrics: ["tracks.unit_price", "invoices.total"],
    filters: [
      {
        operator: "equals",
        member: "genres.name",
        value: ["Rock"],
      },
      { operator: "gt", member: "invoices.total", value: [100] },
    ],
    order: [{ member: "artists.name", direction: "asc" }],
  };

  const processed = processQueryAndExpandToSegments(
    queryBuilder.repository,
    query,
  );

  assert.deepEqual(processed, {
    query: {
      dimensions: ["artists.name"],
      metrics: ["tracks.unit_price", "invoices.total"],
      filters: [
        { operator: "equals", member: "genres.name", value: ["Rock"] },
        {
          member: "invoices.total",
          operator: "gt",
          value: [100],
        },
      ],
      order: [{ member: "artists.name", direction: "asc" }],
    },
    referencedModels: {
      all: ["artists", "tracks", "invoices", "genres"],
      dimensions: ["artists"],
      metrics: ["tracks", "invoices"],
    },
    segments: [
      {
        query: {
          dimensions: ["artists.name"],
          metrics: ["tracks.unit_price"],
          filters: [
            { operator: "equals", member: "genres.name", value: ["Rock"] },
            {
              member: "invoices.total",
              operator: "gt",
              value: [100],
            },
          ],
        },
        projectedQuery: {
          dimensions: ["artists.name"],
          metrics: ["tracks.unit_price"],
          filters: [
            { operator: "equals", member: "genres.name", value: ["Rock"] },
            {
              member: "invoices.total",
              operator: "gt",
              value: [100],
            },
          ],
        },
        referencedModels: {
          all: ["artists", "tracks", "invoices", "genres"],
          dimensions: ["artists"],
          metrics: ["tracks"],
        },
        modelQueries: {
          artists: {
            dimensions: new Set(["artists.name"]),
            metrics: new Set<string>(),
          },
          tracks: {
            dimensions: new Set<string>(),
            metrics: new Set(["tracks.unit_price"]),
          },
        },
        metricModel: "tracks",
      },
      {
        query: {
          dimensions: ["artists.name"],
          metrics: ["invoices.total"],
          filters: [
            { operator: "equals", member: "genres.name", value: ["Rock"] },
            {
              member: "invoices.total",
              operator: "gt",
              value: [100],
            },
          ],
        },
        projectedQuery: {
          dimensions: ["artists.name"],
          metrics: ["invoices.total"],
          filters: [
            { operator: "equals", member: "genres.name", value: ["Rock"] },
            {
              member: "invoices.total",
              operator: "gt",
              value: [100],
            },
          ],
        },
        referencedModels: {
          all: ["artists", "tracks", "invoices", "genres"],
          dimensions: ["artists"],
          metrics: ["invoices"],
        },
        modelQueries: {
          artists: {
            dimensions: new Set(["artists.name"]),
            metrics: new Set<string>(),
          },
          invoices: {
            dimensions: new Set<string>(),
            metrics: new Set(["invoices.total"]),
          },
        },
        metricModel: "invoices",
      },
    ],
  });
});
