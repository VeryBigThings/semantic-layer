import { z } from "zod";
import { filterFragmentBuilder } from "./filter-fragment-builder.js";

function renderILike(
  startsWith: boolean,
  endsWith: boolean,
  negation: boolean,
  memberSql: string,
) {
  let like = "?";
  if (startsWith) {
    like = `'%' || ${like}`;
  }
  if (endsWith) {
    like = `${like} || '%'`;
  }
  if (negation) {
    return `${memberSql} not ilike ${like}`;
  }
  return `${memberSql} ilike ${like}`;
}
function makeILikeFilterBuilder<T extends string>(
  name: T,
  startsWith: boolean,
  endsWith: boolean,
  negation: boolean,
  connective: "and" | "or",
) {
  return filterFragmentBuilder(
    name,
    z.array(z.string()),
    (_filterBuilder, _context, member, filter) => {
      const { sqls, bindings } = filter.value.reduce<{
        sqls: string[];
        bindings: unknown[];
      }>(
        (acc, value) => {
          acc.sqls.push(
            renderILike(startsWith, endsWith, negation, member.sql),
          );
          acc.bindings.push(...member.bindings, value);
          return acc;
        },
        { sqls: [], bindings: [] },
      );

      return {
        sql: `(${sqls.join(` ${connective} `)})`,
        bindings,
      };
    },
  );
}

export const contains = makeILikeFilterBuilder(
  "contains" as const,
  false,
  false,
  false,
  "or",
);
export const notContains = makeILikeFilterBuilder(
  "notContains" as const,
  false,
  false,
  true,
  "and",
);
export const startsWith = makeILikeFilterBuilder(
  "startsWith" as const,
  true,
  false,
  false,
  "or",
);
export const notStartsWith = makeILikeFilterBuilder(
  "notStartsWith" as const,
  true,
  false,
  true,
  "and",
);
export const endsWith = makeILikeFilterBuilder(
  "endsWith" as const,
  false,
  true,
  false,
  "or",
);
export const notEndsWith = makeILikeFilterBuilder(
  "notEndsWith" as const,
  false,
  true,
  true,
  "and",
);
