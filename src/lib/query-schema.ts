import { z } from "zod";
import { AnyQueryBuilder } from "./query-builder.js";
import { AnyQueryFilter } from "./types.js";

function getDimensionNamesSchema(dimensionPaths: string[]) {
  return z
    .array(
      z
        .string()
        .refine((arg) => dimensionPaths.includes(arg))
        .describe("Dimension name"),
    )
    .optional();
}

function getMetricNamesSchema(metricPaths: string[], dimensionPaths: string[]) {
  const adHocMetricSchema = z
    .object({
      aggregateWith: z.enum(["sum", "count", "min", "max", "avg"]),
      dimension: z
        .string()
        .refine((arg) => dimensionPaths.includes(arg))
        .describe("Dimension name"),
    })
    .describe("Ad hoc metric");

  return z
    .array(
      z
        .string()
        .refine((arg) => metricPaths.includes(arg))
        .describe("Metric name")
        .or(adHocMetricSchema),
    )
    .optional();
}

export function buildQuerySchema(queryBuilder: AnyQueryBuilder) {
  const dimensionPaths = queryBuilder.repository
    .getDimensions()
    .map((d) => d.getPath());
  const metricPaths = queryBuilder.repository
    .getMetrics()
    .map((m) => m.getPath());
  const memberPaths = [...dimensionPaths, ...metricPaths];

  const registeredFilterFragmentBuildersSchemas = queryBuilder.repository
    .getFilterFragmentBuilderRegistry()
    .getFilterFragmentBuilders()
    .map((builder) => builder.getFilterFragmentBuilderSchema(queryBuilder));

  const filters: z.ZodType<AnyQueryFilter[]> = z.array(
    z
      .union([
        z
          .object({
            operator: z.literal("and"),
            filters: z.lazy(() => filters),
          })
          .describe("AND connective for filters"),
        z
          .object({
            operator: z.literal("or"),
            filters: z.lazy(() => filters),
          })
          .describe("OR connective for filters"),
        ...registeredFilterFragmentBuildersSchemas.map((schema) =>
          schema.refine((arg) => memberPaths.includes(arg.member), {
            path: ["member"],
            message: "Member not found",
          }),
        ),
      ])
      .describe(
        "Query filters. Top level filters are connected with AND connective. Filters can be nested with AND and OR connectives.",
      ),
  );

  const schema = z
    .object({
      dimensions: getDimensionNamesSchema(dimensionPaths),
      metrics: getMetricNamesSchema(metricPaths, dimensionPaths),
      filters: filters.optional(),
      limit: z.number().optional(),
      offset: z.number().optional(),
      order: z.record(z.string(), z.enum(["asc", "desc"])).optional(),
    })
    .describe("Query schema")
    .refine(
      (arg) => (arg.dimensions?.length ?? 0) + (arg.metrics?.length ?? 0) > 0,
      "At least one dimension or metric must be selected",
    );

  return schema;
}

export type QuerySchema = ReturnType<typeof buildQuerySchema>;
