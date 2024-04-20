import * as chrono from "chrono-node";

import dayjs from "dayjs";
import { z } from "zod";
import { filterFragmentBuilder } from "./filter-fragment-builder.js";

const Schema = z.union([z.string(), z.date()]);
type Schema = z.infer<typeof Schema>;

function parseDate(value: Schema) {
  if (typeof value === "string") {
    const result = dayjs(value).toDate() ?? chrono.parseDate(value);

    if (result) {
      return result;
    }

    throw new Error(`Invalid date: ${value}`);
  }
  return value;
}

function makeDateFilterBuilder<T extends "beforeDate" | "afterDate">(name: T) {
  return filterFragmentBuilder(
    name,
    Schema,
    (_builder, _context, member, filter) => {
      const date = parseDate(filter.value);
      const sql = `${member.sql} ${name === "beforeDate" ? "<" : ">"} ?`;
      const bindings: unknown[] = [...member.bindings, date];
      return {
        sql,
        bindings,
      };
    },
  );
}

export const beforeDate = makeDateFilterBuilder("beforeDate" as const);
export const afterDate = makeDateFilterBuilder("afterDate" as const);
