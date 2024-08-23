import * as fullRepository from "../full-repository.js";

import { expect, it } from "vitest";

import { getQueryPlan } from "../../lib/query-builder/query-plan.js";

it("can crate a query plan", () => {
  const { queryBuilder } = fullRepository;

  const queryPlan = getQueryPlan(queryBuilder.repository, {
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

  expect(queryPlan).toMatchObject({
    segments: [
      {
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
        initialModel: "tracks",
        filters: [
          {
            operator: "equals",
            member: "genres.name",
            value: ["Rock"],
          },
        ],
      },
      {
        models: ["artists", "tracks", "albums", "genres", "invoice_lines"],
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
        initialModel: "invoice_lines",
        filters: [
          {
            operator: "equals",
            member: "genres.name",
            value: ["Rock"],
          },
        ],
      },
      {
        models: ["artists", "tracks", "albums", "genres", "invoices"],
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
        initialModel: "invoices",
        filters: [
          {
            operator: "equals",
            member: "genres.name",
            value: ["Rock"],
          },
        ],
      },
    ],
    filters: [
      {
        operator: "gt",
        member: "invoice_lines.unit_price",
        value: [0],
      },
      {
        operator: "gt",
        member: "invoice_lines.quantity",
        value: [0],
      },
      {
        operator: "gt",
        member: "tracks.unit_price",
        value: [0],
      },
      {
        operator: "gt",
        member: "invoices.total",
        value: [100],
      },
    ],
    projectedDimensions: ["artists.name", "tracks.name", "albums.title"],
    projectedMetrics: ["tracks.unit_price", "invoice_lines.quantity"],
    order: [
      {
        member: "artists.name",
        direction: "asc",
      },
    ],
  });
});
