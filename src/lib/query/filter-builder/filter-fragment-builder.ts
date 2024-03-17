import { ZodSchema, z } from "zod";

import { SqlWithBindings } from "../../../types.js";
import type { FilterBuilder } from "../filter-builder.js";

export class FilterFragmentBuilder<
  N extends string,
  Z extends ZodSchema | null,
  T extends FilterFragmentBuilderPayload<N, Z>,
> {
  public readonly fragmentBuilderSchema: ZodSchema;
  constructor(
    public readonly operator: string,
    valueSchema: ZodSchema | null,
    private readonly builder: FilterFragmentBuilderFn<T>,
  ) {
    if (valueSchema) {
      this.fragmentBuilderSchema = z.object({
        operator: z.literal(operator),
        member: z.string(),
        value: valueSchema,
      });
    } else {
      this.fragmentBuilderSchema = z.object({
        operator: z.literal(operator),
        member: z.string(),
      });
    }
  }
  build(filterBuilder: FilterBuilder, member: SqlWithBindings, payload: T) {
    const filter = this.fragmentBuilderSchema.parse(payload);
    return this.builder(filterBuilder, member, filter);
  }
}

export type AnyFilterFragmentBuilder = FilterFragmentBuilder<
  string,
  ZodSchema | null,
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  any
>;

type FilterFragmentBuilderFn<T> = (
  builder: FilterBuilder,
  member: SqlWithBindings,
  filter: T,
) => SqlWithBindings;

export type GetFilterFragmentBuilderPayload<T> =
  T extends FilterFragmentBuilder<string, ZodSchema, infer P> ? P : never;

type FilterFragmentBuilderPayload<
  N extends string,
  Z extends ZodSchema | null,
  T = Z extends ZodSchema ? z.infer<Z> : null,
> = T extends null
  ? { operator: N; member: string }
  : {
      operator: N;
      member: string;
      value: T;
    };

export function filterFragmentBuilder<
  N extends string,
  Z extends ZodSchema | null,
  T extends FilterFragmentBuilderPayload<N, Z>,
>(name: N, valueSchema: Z, builder: FilterFragmentBuilderFn<T>) {
  return new FilterFragmentBuilder(name, valueSchema, builder);
}
