import { z } from "zod";
import { SqlFragment } from "../../sql-builder.js";
import { filterFragmentBuilder } from "./filter-fragment-builder.js";

const OPERATOR_MAPPING = {
  gt: ">",
  gte: ">=",
  lt: "<",
  lte: "<=",
} as const;

const DOCUMENTATION: Record<keyof typeof OPERATOR_MAPPING, string> = {
  gt: "Filter for values that are greater than the given value.",
  gte: "Filter for values that are greater than or equal to the given value.",
  lt: "Filter for values that are less than the given value.",
  lte: "Filter for values that are less than or equal to the given value.",
} as const;

function makeNumberComparisonFilterBuilder<
  T extends keyof typeof OPERATOR_MAPPING,
>(operator: T) {
  return filterFragmentBuilder(
    operator,
    DOCUMENTATION[operator],
    z.array(z.number({ coerce: true })).min(1),
    (_builder, _context, member, filter) => {
      const { sqls, bindings } = filter.value.reduce<{
        sqls: string[];
        bindings: unknown[];
      }>(
        (acc, value) => {
          acc.sqls.push(`${member.sql} ${OPERATOR_MAPPING[operator]} ?`);
          acc.bindings.push(...member.bindings, value);
          return acc;
        },
        { sqls: [], bindings: [] },
      );

      return SqlFragment.make({
        sql: `(${sqls.join(" and ")})`,
        bindings,
      });
    },
  );
}

export const gt = makeNumberComparisonFilterBuilder("gt" as const);
export const gte = makeNumberComparisonFilterBuilder("gte" as const);
export const lt = makeNumberComparisonFilterBuilder("lt" as const);
export const lte = makeNumberComparisonFilterBuilder("lte" as const);
