import {
  FilterFragmentBuilder,
  filterFragmentBuilder,
} from "./filter-fragment-builder.js";

import { z } from "zod";
import { AnyQueryBuilder } from "../../query-builder.js";

export type InOrNotIn = "in" | "notIn";

const inOrNotInToSQL = {
  in: "in",
  notIn: "not in",
} as const;

// Return type here is intentionally simplified, but we make it exact later in the QueryBuilder class
function makeQueryFilterFragmentBuilder<T extends string>(
  name: T,
  inOrNotIn: InOrNotIn,
): FilterFragmentBuilder<
  T,
  (queryBuilder: AnyQueryBuilder) => z.ZodType<object>,
  {
    operator: T;
    member: string;
    value: object;
  }
> {
  return filterFragmentBuilder(
    name,
    (queryBuilder) => {
      return z.lazy(() => queryBuilder.querySchema);
    },
    (
      filterBuilder,
      context,
      member,
      filter,
    ): { sql: string; bindings: unknown[] } => {
      const { sql, bindings } =
        filterBuilder.queryBuilder.unsafeBuildGenericQueryWithoutSchemaParse(
          filter.value,
          context,
        );

      return {
        sql: `${member.sql} ${inOrNotInToSQL[inOrNotIn]} (${sql})`,
        bindings: [...member.bindings, ...bindings],
      };
    },
  );
}

export const inQuery = makeQueryFilterFragmentBuilder("inQuery", "in");
export const notInQuery = makeQueryFilterFragmentBuilder("notInQuery", "notIn");
