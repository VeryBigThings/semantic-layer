import { filterFragmentBuilder } from "./filter-fragment-builder.js";
import { z } from "zod";

const inOrNotInToSQL = {
  in: "in",
  notIn: "not in",
} as const;

function makeQueryFilterFragmentBuilder<T extends string>(
  name: T,
  inOrNotIn: "in" | "notIn",
) {
  return filterFragmentBuilder(
    name,
    (queryBuilder) => {
      return z.lazy(() => queryBuilder.querySchema);
    },
    (filterBuilder, member, filter): { sql: string; bindings: unknown[] } => {
      const { sql, bindings } =
        filterBuilder.queryBuilder.unsafeBuildGenericQueryWithoutSchemaParse(
          filter.value,
          undefined,
        );

      return {
        sql: `${member.sql} ${inOrNotInToSQL[inOrNotIn]} (${sql})`,
        bindings: [...bindings],
      };
    },
  );
}

export const inQuery = makeQueryFilterFragmentBuilder("inQuery", "in");
export const notInQuery = makeQueryFilterFragmentBuilder("notInQuery", "notIn");
