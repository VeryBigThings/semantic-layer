import * as chrono from "chrono-node";

import dayjs from "dayjs";
import { z } from "zod";
import { SqlFragment } from "../../sql-builder.js";
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

const DOCUMENTATION = {
  beforeDate:
    'Filter for dates before the given date. Accepts a value as date, date formatted as a string or a string with relative time like "start of last year".',
  afterDate:
    'Filter for dates after the given date. Accepts a value as date, date formatted as a string or a string with relative time like "start of last year".',
} as const;

function makeDateFilterBuilder<T extends keyof typeof DOCUMENTATION>(name: T) {
  return filterFragmentBuilder(
    name,
    DOCUMENTATION[name],
    Schema,
    (_builder, _context, member, filter) => {
      const date = parseDate(filter.value);
      const sql = `${member.sql} ${name === "beforeDate" ? "<" : ">"} ?`;
      const bindings: unknown[] = [...member.bindings, date];
      return SqlFragment.make({
        sql,
        bindings,
      });
    },
  );
}

export const beforeDate = makeDateFilterBuilder("beforeDate" as const);
export const afterDate = makeDateFilterBuilder("afterDate" as const);
