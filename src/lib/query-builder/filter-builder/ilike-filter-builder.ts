import { z } from "zod";
import { SqlFragment } from "../../sql-builder.js";
import { filterFragmentBuilder } from "./filter-fragment-builder.js";

const DOCUMENTATION = {
  contains:
    "Filter for values that contain the given string. Accepts an array of strings.",
  notContains:
    "Filter for values that do not contain the given string. Accepts an array of strings.",
  startsWith:
    "Filter for values that start with the given string. Accepts an array of strings.",
  notStartsWith:
    "Filter for values that do not start with the given string. Accepts an array of strings.",
  endsWith:
    "Filter for values that end with the given string. Accepts an array of strings.",
  notEndsWith:
    "Filter for values that do not end with the given string. Accepts an array of strings.",
} as const;

function makeILikeFilterBuilder<T extends keyof typeof DOCUMENTATION>(
  name: T,
  beginWithWildcard: boolean,
  endWithWildcard: boolean,
  negation: boolean,
  connective: "and" | "or",
) {
  return filterFragmentBuilder(
    name,
    DOCUMENTATION[name],
    z.array(z.string()).min(1),
    (filterBuilder, _context, member, filter) => {
      const { sqls, bindings } = filter.value.reduce<{
        sqls: string[];
        bindings: unknown[];
      }>(
        (acc, value) => {
          acc.sqls.push(
            filterBuilder.queryBuilder.dialect.ilike(
              beginWithWildcard,
              endWithWildcard,
              negation,
              member.sql,
            ),
          );
          acc.bindings.push(...member.bindings, value);
          return acc;
        },
        { sqls: [], bindings: [] },
      );

      return SqlFragment.make({
        sql: `(${sqls.join(` ${connective} `)})`,
        bindings,
      });
    },
  );
}

export const contains = makeILikeFilterBuilder(
  "contains" as const,
  true,
  true,
  false,
  "or",
);
export const notContains = makeILikeFilterBuilder(
  "notContains" as const,
  true,
  true,
  true,
  "and",
);
export const startsWith = makeILikeFilterBuilder(
  "startsWith" as const,
  false,
  true,
  false,
  "or",
);
export const notStartsWith = makeILikeFilterBuilder(
  "notStartsWith" as const,
  false,
  true,
  true,
  "and",
);
export const endsWith = makeILikeFilterBuilder(
  "endsWith" as const,
  true,
  false,
  false,
  "or",
);
export const notEndsWith = makeILikeFilterBuilder(
  "notEndsWith" as const,
  true,
  false,
  true,
  "and",
);
