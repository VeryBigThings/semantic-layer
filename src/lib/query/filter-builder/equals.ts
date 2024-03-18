import { z } from "zod";
import { filterFragmentBuilder } from "./filter-fragment-builder.js";

export const equals = filterFragmentBuilder(
  "equals",
  z.array(z.union([z.string(), z.number(), z.bigint(), z.boolean(), z.date()])),
  (_builder, member, filter) => {
    if (filter.value.length === 1) {
      return {
        sql: `${member.sql} = ?`,
        bindings: [...member.bindings, filter.value[0]],
      };
    }
    return {
      sql: `${member.sql} in (${filter.value.map(() => "?").join(", ")})`,
      bindings: [...member.bindings, ...filter.value],
    };
  },
);
