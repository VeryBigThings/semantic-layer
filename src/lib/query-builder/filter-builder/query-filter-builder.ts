import {
  FilterFragmentBuilder,
  filterFragmentBuilder,
} from "./filter-fragment-builder.js";

import { z } from "zod";
import { AnyQueryBuilder } from "../../query-builder.js";
import { SqlFragment } from "../../sql-builder.js";

export type InOrNotIn = "in" | "notIn";

const inOrNotInToSQL = {
  in: "in",
  notIn: "not in",
} as const;

const DOCUMENTATION = {
  inQuery: "Filter for values that are in the result of the given query.",
  notInQuery:
    "Filter for values that are not in the result of the given query.",
} as const;

// Return type here is intentionally simplified, but we make it exact later in the QueryBuilder class
function makeQueryFilterFragmentBuilder<T extends keyof typeof DOCUMENTATION>(
  name: T,
  inOrNotIn: InOrNotIn,
): FilterFragmentBuilder<
  T,
  (queryBuilder: AnyQueryBuilder) => z.ZodType<object>,
  {
    operator: T;
    member: string;
    value: any;
  }
> {
  return filterFragmentBuilder(
    name,
    DOCUMENTATION[name],
    (queryBuilder) => {
      return z.lazy(() => queryBuilder.querySchema);
    },
    (filterBuilder, context, member, filter): SqlFragment => {
      const { sql, bindings } =
        filterBuilder.queryBuilder.unsafeBuildGenericQueryWithoutSchemaParse(
          filter.value,
          context,
        );

      return SqlFragment.make({
        sql: `${member.sql} ${inOrNotInToSQL[inOrNotIn]} (${sql})`,
        bindings: [...member.bindings, ...bindings],
      });
    },
  );
}

export const inQuery = makeQueryFilterFragmentBuilder("inQuery", "in");
export const notInQuery = makeQueryFilterFragmentBuilder("notInQuery", "notIn");
