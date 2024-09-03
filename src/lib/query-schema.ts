import { AnyZodObject, z } from "zod";

import { AnyQueryBuilder } from "./query-builder.js";
import { AnyQueryFilter } from "./types.js";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function buildQuerySchema(queryBuilder: AnyQueryBuilder) {
  const dimensionPaths = queryBuilder.repository
    .getDimensions()
    .filter((d) => !d.isPrivate())
    .map((d) => d.getPath());
  const metricPaths = queryBuilder.repository
    .getMetrics()
    .filter((m) => !m.isPrivate())
    .map((m) => m.getPath());

  const memberToTypeIndex = {
    ...dimensionPaths.reduce<Record<string, "dimension">>((acc, path) => {
      acc[path] = "dimension";
      return acc;
    }, {}),
    ...metricPaths.reduce<Record<string, "metric">>((acc, path) => {
      acc[path] = "metric";
      return acc;
    }, {}),
  };

  const memberPaths = [...dimensionPaths, ...metricPaths];

  const registeredFilterFragmentBuildersSchemas = queryBuilder.repository
    .getFilterFragmentBuilderRegistry()
    .getFilterFragmentBuilders()
    .map((builder) => {
      const filter = builder.getFilterFragmentBuilderSchema(
        queryBuilder,
      ) as AnyZodObject;

      const mergedFilter = filter.merge(
        z.object({
          member: z.string().refine((arg) => memberPaths.includes(arg), {
            message: "Member not found",
          }),
        }),
      ) as typeof filter;

      return filter.description
        ? mergedFilter.describe(filter.description)
        : mergedFilter;
    });

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Essential complexity for validating filters. We need to validate that all and/or connectives have filters with members of the same type (dimension or metric), and we need to do this recursively. Using a queue to avoid stack overflows (although it's not a big chance that we'll have more than a few levels of nesting).
  const getConnectiveFilterMemberTypes = (filters: AnyQueryFilter[]) => {
    const connectiveFiltersMemberTypes: Set<"metric" | "dimension"> = new Set();
    const filtersToProcess = [...filters];

    while (filtersToProcess.length > 0) {
      const filter = filtersToProcess.shift();
      // We don't have any type safety here because at this moment we don't know which filters are registered (although we should in practice have two types of structures - filters which should all have format of {operator: string, member: string, ...} or connectives which should have a format of {operator: "and" | "or", filters: [...]}), so we do some extra checks //
      if (isRecord(filter)) {
        if (
          filter.operator === "and" ||
          (filter.operator === "or" && Array.isArray(filter.filters))
        ) {
          const subFilters = filter.filters as AnyQueryFilter[];
          filtersToProcess.push(...subFilters);
        } else {
          const member =
            typeof filter.member === "string" ? filter.member : null;
          if (member) {
            const memberType = memberToTypeIndex[member];
            memberType && connectiveFiltersMemberTypes.add(memberType);
          }
        }
      }
    }

    return connectiveFiltersMemberTypes;
  };

  function validateConnectiveFiltersMemberTypes(filters: AnyQueryFilter[]) {
    return getConnectiveFilterMemberTypes(filters).size < 2;
  }

  const invalidConnectedFiltersMessage =
    "All and/or connectives must include filters with members of the same type (dimension or metric)";

  const filters: z.ZodType<AnyQueryFilter[]> = z.array(
    z
      .discriminatedUnion("operator", [
        z
          .object({
            operator: z.literal("and"),
            filters: z.lazy(() =>
              filters.refine(
                (filters) => validateConnectiveFiltersMemberTypes(filters),
                {
                  message: invalidConnectedFiltersMessage,
                },
              ),
            ),
          })
          .describe("AND connective for filters"),
        z
          .object({
            operator: z.literal("or"),
            filters: z.lazy(() =>
              filters.refine(
                (filters) => validateConnectiveFiltersMemberTypes(filters),
                {
                  message: invalidConnectedFiltersMessage,
                },
              ),
            ),
          })
          .describe("OR connective for filters"),
        ...(registeredFilterFragmentBuildersSchemas as z.ZodDiscriminatedUnionOption<"operator">[]),
      ])
      .describe(
        "Query filters. Top level filters are connected with AND connective. Filters can be nested with AND and OR connectives.",
      ),
  );

  const schema = z
    .object({
      members: z
        .array(
          z
            .string()
            .refine((arg) => memberPaths.includes(arg), {
              message: "Member not found",
            })
            .describe("Dimension or metric name"),
        )
        .min(1),
      limit: z.number().optional(),
      offset: z.number().optional(),
      order: z
        .array(
          z.object({
            member: z.string().refine((arg) => memberPaths.includes(arg), {
              message: "Member not found",
            }),
            direction: z.enum(["asc", "desc"]),
          }),
        )
        .optional(),
      filters: filters.optional(),
    })
    .describe("Query schema");

  return schema;
}

export type QuerySchema = ReturnType<typeof buildQuerySchema>;
