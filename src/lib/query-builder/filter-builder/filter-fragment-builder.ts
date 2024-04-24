import { ZodSchema, z } from "zod";

import { AnyQueryBuilder } from "../../query-builder.js";
import { SqlWithBindings } from "../../types.js";
import type { FilterBuilder } from "../filter-builder.js";

export class FilterFragmentBuilder<
  N extends string,
  Z extends ZodSchema | ((queryBuilder: AnyQueryBuilder) => ZodSchema) | null,
  T extends FilterFragmentBuilderPayload<N, Z>,
> {
  public readonly fragmentBuilderSchema:
    | ZodSchema
    | ((queryBuilder: AnyQueryBuilder) => ZodSchema);
  constructor(
    public readonly operator: string,
    filterSchemaDescription: string,
    valueSchema: Z,
    private readonly builder: FilterFragmentBuilderFn<T>,
  ) {
    if (valueSchema) {
      if (typeof valueSchema === "function") {
        this.fragmentBuilderSchema = (queryBuilder: AnyQueryBuilder) => {
          const resolvedValueSchema = valueSchema(queryBuilder);
          return z
            .object({
              operator: z.literal(operator),
              member: z.string(),
              value: resolvedValueSchema,
            })
            .describe(filterSchemaDescription);
        };
      } else {
        this.fragmentBuilderSchema = z
          .object({
            operator: z.literal(operator),
            member: z.string(),
            value: valueSchema,
          })
          .describe(filterSchemaDescription);
      }
    } else {
      this.fragmentBuilderSchema = z
        .object({
          operator: z.literal(operator),
          member: z.string(),
        })
        .describe(filterSchemaDescription);
    }
  }
  getFilterFragmentBuilderSchema(queryBuilder: AnyQueryBuilder) {
    return typeof this.fragmentBuilderSchema === "function"
      ? this.fragmentBuilderSchema(queryBuilder)
      : this.fragmentBuilderSchema;
  }
  build(
    filterBuilder: FilterBuilder,
    context: unknown,
    member: SqlWithBindings,
    payload: unknown,
  ) {
    // We can directly pass payload as T because the schema is already validated in the QueryBuilder
    return this.builder(filterBuilder, context, member, payload as T);
  }
}

export type AnyFilterFragmentBuilder = FilterFragmentBuilder<
  string,
  ZodSchema | ((queryBuilder: AnyQueryBuilder) => ZodSchema) | null,
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  any
>;

export type FilterFragmentBuilderFn<T> = (
  builder: FilterBuilder,
  context: unknown,
  member: SqlWithBindings,
  filter: T,
) => SqlWithBindings;

export type GetFilterFragmentBuilderPayload<T> =
  T extends FilterFragmentBuilder<string, ZodSchema, infer P> ? P : never;

export type FilterFragmentBuilderPayload<
  N extends string,
  Z extends ZodSchema | ((queryBuilder: AnyQueryBuilder) => ZodSchema) | null,
  T = Z extends ZodSchema
    ? z.infer<Z>
    : Z extends (queryBuilder: AnyQueryBuilder) => ZodSchema
      ? z.infer<ReturnType<Z>>
      : null,
> = T extends null
  ? { operator: N; member: string }
  : {
      operator: N;
      member: string;
      value: T;
    };

export function filterFragmentBuilder<
  N extends string,
  Z extends ZodSchema | ((queryBuilder: AnyQueryBuilder) => ZodSchema) | null,
  T extends FilterFragmentBuilderPayload<N, Z>,
>(
  name: N,
  filterSchemaDescription: string,
  valueSchema: Z,
  builder: FilterFragmentBuilderFn<T>,
) {
  return new FilterFragmentBuilder(
    name,
    filterSchemaDescription,
    valueSchema,
    builder,
  );
}
