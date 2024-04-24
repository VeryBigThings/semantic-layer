import { filterFragmentBuilder } from "./filter-fragment-builder.js";

const DOCUMENTATION = {
  set: "Filter for values that are set.",
  notSet: "Filter for values that are not set.",
} as const;

function makeNullCheckFilterBuilder<T extends keyof typeof DOCUMENTATION>(
  name: T,
  isNull: boolean,
) {
  return filterFragmentBuilder(
    name,
    DOCUMENTATION[name],
    null,
    (_builder, _context, member) => {
      const sql = `${member.sql} is ${isNull ? "" : "not"} null`;
      return {
        sql,
        bindings: [...member.bindings],
      };
    },
  );
}

export const set = makeNullCheckFilterBuilder("set" as const, false);
export const notSet = makeNullCheckFilterBuilder("notSet" as const, true);
