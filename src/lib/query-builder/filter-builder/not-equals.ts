import { z } from "zod";
import { filterFragmentBuilder } from "./filter-fragment-builder.js";

const DOCUMENTATION =
  "Filter for values that are not equal to the given value. Accepts an array of values. If the array contains more than one value, the filter will return rows where the member is not equal to any of the values.";

function makeNotEqualsFilterFragmentBuilder<T extends string>(name: T) {
  return filterFragmentBuilder(
    name,
    DOCUMENTATION,
    z.array(
      z.union([z.string(), z.number(), z.bigint(), z.boolean(), z.date()]),
    ),
    (_builder, _context, member, filter) => {
      if (filter.value.length === 1) {
        return {
          sql: `${member.sql} <> ?`,
          bindings: [...member.bindings, filter.value[0]],
        };
      }
      return {
        sql: `${member.sql} not in (${filter.value.map(() => "?").join(", ")})`,
        bindings: [...member.bindings, ...filter.value],
      };
    },
  );
}

export const notEquals = makeNotEqualsFilterFragmentBuilder("notEquals");
export const notIn = makeNotEqualsFilterFragmentBuilder("notIn");
