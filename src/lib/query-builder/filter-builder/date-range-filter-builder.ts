import * as chrono from "chrono-node";

import dayjs from "dayjs";
import { z } from "zod";
import { SqlFragment } from "../../sql-builder.js";
import { filterFragmentBuilder } from "./filter-fragment-builder.js";

const Schema = z.union([
  z.string(),
  z.object({
    startDate: z.union([z.string(), z.date()]),
    endDate: z.union([z.string(), z.date()]),
  }),
]);
type Schema = z.infer<typeof Schema>;

function parseDateRange(value: Schema) {
  if (typeof value === "string") {
    const result = chrono.parse(value);
    if (result.length === 0) {
      throw new Error(`Invalid date: ${value}`);
    }
    const firstDate = result[0]!.start.date();
    const lastDate =
      result[result.length - 1]!.end?.date() ??
      result[result.length - 1]!.start.date();
    return [firstDate, lastDate];
  }
  const { startDate: startDateRaw, endDate: endDateRaw } = value;
  const startDate =
    typeof startDateRaw === "string"
      ? dayjs(startDateRaw).toDate()
      : startDateRaw;
  const endDate =
    typeof endDateRaw === "string" ? dayjs(endDateRaw).toDate() : endDateRaw;

  if (!(startDate && endDate)) {
    throw new Error(`Invalid date range: ${JSON.stringify(value)}`);
  }

  return [startDate, endDate];
}

const DOCUMENTATION = {
  inDateRange:
    "Filter for dates in the given range. Accepts a value as date range, date range formatted as a string or an object with startDate and endDate properties.",
  notInDateRange:
    "Filter for dates not in the given range. Accepts a value as date range, date range formatted as a string or an object with startDate and endDate properties.",
} as const;

function makeDateRangeFilterBuilder<T extends keyof typeof DOCUMENTATION>(
  name: T,
  isNot: boolean,
) {
  return filterFragmentBuilder(
    name,
    DOCUMENTATION[name],
    Schema,
    (_builder, _context, member, filter) => {
      const [firstDate, lastDate] = parseDateRange(filter.value);
      const sql = `${member.sql} ${isNot ? "not between" : "between"} ? and ?`;
      const bindings: unknown[] = [...member.bindings, firstDate, lastDate];
      return SqlFragment.make({
        sql,
        bindings,
      });
    },
  );
}

export const inDateRange = makeDateRangeFilterBuilder(
  "inDateRange" as const,
  false,
);
export const notInDateRange = makeDateRangeFilterBuilder(
  "notInDateRange" as const,
  true,
);
