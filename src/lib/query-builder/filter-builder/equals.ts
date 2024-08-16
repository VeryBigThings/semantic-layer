import { z } from "zod";
import { SqlFragment } from "../../sql-builder.js";
import { filterFragmentBuilder } from "./filter-fragment-builder.js";

const DOCUMENTATION =
  "Filter for values that are equal to the given value. Accepts an array of values. If the array contains more than one value, the filter will return rows where the member is equal to any of the values.";

function makeEqualsFilterFragmentBuilder<T extends string>(name: T) {
  return filterFragmentBuilder(
    name,
    DOCUMENTATION,
    z
      .array(
        z.union([z.string(), z.number(), z.bigint(), z.boolean(), z.date()]),
      )
      .min(1),
    (_builder, _context, member, filter) => {
      if (filter.value.length === 1) {
        return SqlFragment.make({
          sql: `${member.sql} = ?`,
          bindings: [...member.bindings, filter.value[0]],
        });
      }
      return SqlFragment.make({
        sql: `${member.sql} in (${filter.value.map(() => "?").join(", ")})`,
        bindings: [...member.bindings, ...filter.value],
      });
    },
  );
}

export const equals = makeEqualsFilterFragmentBuilder("equals");
export const filterIn = makeEqualsFilterFragmentBuilder("in");
