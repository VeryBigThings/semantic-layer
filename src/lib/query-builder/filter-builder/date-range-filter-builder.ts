import * as chrono from "chrono-node";

import dayjs from "dayjs";
import { z } from "zod";
import { filterFragmentBuilder } from "./filter-fragment-builder.js";

const Schema = z.union([
  z.string(),
  z.tuple([z.union([z.string(), z.date()]), z.union([z.string(), z.date()])]),
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
  const [firstDateRaw, lastDateRaw] = value;
  const firstDate =
    typeof firstDateRaw === "string"
      ? dayjs(firstDateRaw).toDate()
      : firstDateRaw;
  const lastDate =
    typeof lastDateRaw === "string" ? dayjs(lastDateRaw).toDate() : lastDateRaw;

  if (!(firstDate && lastDate)) {
    throw new Error(`Invalid date range: ${JSON.stringify(value)}`);
  }

  return [firstDate, lastDate];
}

function makeDateRangeFilterBuilder<T extends string>(name: T, isNot: boolean) {
  return filterFragmentBuilder(name, Schema, (_builder, member, filter) => {
    const [firstDate, lastDate] = parseDateRange(filter.value);
    const sql = `${member.sql} ${isNot ? "not between" : "between"} ? and ?`;
    const bindings: unknown[] = [...member.bindings, firstDate, lastDate];
    return {
      sql,
      bindings,
    };
  });
}

export const inDateRange = makeDateRangeFilterBuilder(
  "inDateRange" as const,
  false,
);
export const notInDateRange = makeDateRangeFilterBuilder(
  "notInDateRange" as const,
  true,
);
