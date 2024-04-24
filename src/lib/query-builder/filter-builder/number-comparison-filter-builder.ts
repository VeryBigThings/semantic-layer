import { z } from "zod";
import { filterFragmentBuilder } from "./filter-fragment-builder.js";

const OPERATOR_MAPPING = {
  gt: ">",
  gte: ">=",
  lt: "<",
  lte: "<=",
} as const;

function makeNumberComparisonFilterBuilder<
  T extends keyof typeof OPERATOR_MAPPING,
>(operator: T) {
  return filterFragmentBuilder(
    operator,
    z.array(z.number({ coerce: true })),
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

      return {
        sql: `(${sqls.join(" and ")})`,
        bindings,
      };
    },
  );
}

export const gt = makeNumberComparisonFilterBuilder("gt" as const);
export const gte = makeNumberComparisonFilterBuilder("gte" as const);
export const lt = makeNumberComparisonFilterBuilder("lt" as const);
export const lte = makeNumberComparisonFilterBuilder("lte" as const);
