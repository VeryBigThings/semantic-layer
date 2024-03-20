import { filterFragmentBuilder } from "./filter-fragment-builder.js";

function makeNullCheckFilterBuilder<T extends string>(
  name: T,
  isNull: boolean,
) {
  return filterFragmentBuilder(name, null, (_builder, member) => {
    const sql = `${member.sql} is ${isNull ? "" : "not"} null`;
    return {
      sql,
      bindings: [...member.bindings],
    };
  });
}

export const set = makeNullCheckFilterBuilder("set" as const, false);
export const notSet = makeNullCheckFilterBuilder("notSet" as const, true);
