import * as fullRepository from "../full-repository.js";

import { assert, it } from "vitest";

import { getQueryPlan } from "../../lib/query-builder/query-plan.js";
import { QueryContext } from "../../lib/query-builder/query-plan/query-context.js";

it("can crate a query plan", () => {
  const { queryBuilder } = fullRepository;
  const queryContext = new QueryContext(
    queryBuilder.repository,
    queryBuilder.dialect,
    undefined,
  );
  const queryPlan = getQueryPlan(queryBuilder, queryContext, undefined, {
    members: [
      "artists.name",
      "tracks.name",
      "albums.title",
      "tracks.unit_price",
      "invoice_lines.quantity",
    ],
    filters: [
      {
        operator: "equals",
        member: "genres.name",
        value: ["Rock"],
      },
      { operator: "gt", member: "invoice_lines.unit_price", value: [0] },
      { operator: "gt", member: "invoice_lines.quantity", value: [0] },
      { operator: "gt", member: "tracks.unit_price", value: [0] },
      { operator: "gt", member: "invoices.total", value: [100] },
    ],
    order: [{ member: "artists.name", direction: "asc" }],
  });

  assert.deepEqual(queryPlan, {
    segments: [
      {
        metricsRefsSubQueryPlan: undefined,
        models: ["artists", "tracks", "albums", "genres"],
        modelQuery: {
          dimensions: [
            "artists.name",
            "tracks.name",
            "albums.title",
            "genres.name",
            "tracks.track_id",
          ],
          metrics: ["tracks.unit_price"],
          members: [
            "artists.name",
            "tracks.name",
            "albums.title",
            "genres.name",
            "tracks.track_id",
            "tracks.unit_price",
          ],
        },
        segmentQuery: {
          dimensions: ["artists.name", "tracks.name", "albums.title"],
          metrics: ["tracks.unit_price"],
          members: [
            "artists.name",
            "tracks.name",
            "albums.title",
            "tracks.unit_price",
          ],
        },
        rootQuery: {
          dimensions: ["artists.name", "tracks.name", "albums.title"],
          metrics: ["tracks.unit_price"],
          members: [
            "artists.name",
            "tracks.name",
            "albums.title",
            "tracks.unit_price",
          ],
        },
        alias: "s0",
        joinPlan: {
          hasRowMultiplication: false,
          initialModel: "tracks",
          joins: [
            {
              leftModel: "tracks",
              rightModel: "albums",
              joinType: "right",
            },
            {
              leftModel: "albums",
              rightModel: "artists",
              joinType: "left",
            },
            { leftModel: "tracks", rightModel: "genres", joinType: "left" },
          ],
        },
        filters: [
          { operator: "equals", member: "genres.name", value: ["Rock"] },
        ],
      },
      {
        models: ["artists", "tracks", "albums", "genres", "invoice_lines"],
        metricsRefsSubQueryPlan: undefined,
        modelQuery: {
          dimensions: [
            "artists.name",
            "tracks.name",
            "albums.title",
            "genres.name",
            "invoice_lines.invoice_line_id",
          ],
          metrics: ["invoice_lines.quantity", "invoice_lines.unit_price"],
          members: [
            "artists.name",
            "tracks.name",
            "albums.title",
            "genres.name",
            "invoice_lines.invoice_line_id",
            "invoice_lines.quantity",
            "invoice_lines.unit_price",
          ],
        },
        segmentQuery: {
          dimensions: ["artists.name", "tracks.name", "albums.title"],
          metrics: ["invoice_lines.quantity", "invoice_lines.unit_price"],
          members: [
            "artists.name",
            "tracks.name",
            "albums.title",
            "invoice_lines.quantity",
            "invoice_lines.unit_price",
          ],
        },
        rootQuery: {
          dimensions: ["artists.name", "tracks.name", "albums.title"],
          metrics: ["invoice_lines.quantity"],
          members: [
            "artists.name",
            "tracks.name",
            "albums.title",
            "invoice_lines.quantity",
          ],
        },
        alias: "s1",
        joinPlan: {
          hasRowMultiplication: false,
          initialModel: "invoice_lines",
          joins: [
            {
              leftModel: "invoice_lines",
              rightModel: "tracks",
              joinType: "left",
            },
            {
              leftModel: "tracks",
              rightModel: "albums",
              joinType: "right",
            },
            {
              leftModel: "albums",
              rightModel: "artists",
              joinType: "left",
            },
            { leftModel: "tracks", rightModel: "genres", joinType: "left" },
          ],
        },
        filters: [
          { operator: "equals", member: "genres.name", value: ["Rock"] },
        ],
      },
      {
        models: ["artists", "tracks", "albums", "genres", "invoices"],
        metricsRefsSubQueryPlan: undefined,
        modelQuery: {
          dimensions: [
            "artists.name",
            "tracks.name",
            "albums.title",
            "genres.name",
            "invoices.invoice_id",
          ],
          metrics: ["invoices.total"],
          members: [
            "artists.name",
            "tracks.name",
            "albums.title",
            "genres.name",
            "invoices.invoice_id",
            "invoices.total",
          ],
        },
        segmentQuery: {
          dimensions: ["artists.name", "tracks.name", "albums.title"],
          metrics: ["invoices.total"],
          members: [
            "artists.name",
            "tracks.name",
            "albums.title",
            "invoices.total",
          ],
        },
        rootQuery: {
          dimensions: ["artists.name", "tracks.name", "albums.title"],
          metrics: [],
          members: ["artists.name", "tracks.name", "albums.title"],
        },
        alias: "s2",
        joinPlan: {
          hasRowMultiplication: true,
          initialModel: "invoices",
          joins: [
            {
              leftModel: "invoices",
              rightModel: "invoice_lines",
              joinType: "left",
            },
            {
              leftModel: "invoice_lines",
              rightModel: "tracks",
              joinType: "left",
            },
            {
              leftModel: "tracks",
              rightModel: "albums",
              joinType: "right",
            },
            {
              leftModel: "albums",
              rightModel: "artists",
              joinType: "left",
            },
            { leftModel: "tracks", rightModel: "genres", joinType: "left" },
          ],
        },
        filters: [
          { operator: "equals", member: "genres.name", value: ["Rock"] },
        ],
      },
    ],
    filters: [
      { operator: "gt", member: "invoice_lines.unit_price", value: [0] },
      { operator: "gt", member: "invoice_lines.quantity", value: [0] },
      { operator: "gt", member: "tracks.unit_price", value: [0] },
      { operator: "gt", member: "invoices.total", value: [100] },
    ],
    projectedDimensions: ["artists.name", "tracks.name", "albums.title"],
    projectedMetrics: ["tracks.unit_price", "invoice_lines.quantity"],
    order: [{ member: "artists.name", direction: "asc" }],
    limit: undefined,
    offset: undefined,
  });
});
