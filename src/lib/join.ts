import { ColumnRef, DimensionRef, IdentifierRef, SqlFn } from "./sql-fn.js";

import { AnyModel } from "./model.js";
import { ModelMemberWithoutModelPrefix } from "./types.js";

export interface Join<C> {
  left: string;
  right: string;
  joinOnDef: (context: C) => SqlFn;
  reversed: boolean;
  type: "oneToOne" | "oneToMany" | "manyToOne" | "manyToMany";
}

export type AnyJoin = Join<any>;

export type JoinFn<
  C,
  DN extends string,
  N1 extends string,
  N2 extends string,
> = (args: {
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlFn;
  models: JoinFnModels<DN, N1 | N2>;
  identifier: (name: string) => IdentifierRef;
  getContext: () => C;
}) => SqlFn;

export type JoinFnModels<
  TDimensionName extends string,
  TModelName extends string,
> = {
  [TK in TModelName]: {
    dimension: (
      dimensionName: ModelMemberWithoutModelPrefix<TK, TDimensionName>,
    ) => DimensionRef;
    column: (columnName: string) => ColumnRef;
  };
};

export function makeModelJoinPayload(model: AnyModel, context: unknown) {
  return {
    dimension: (dimensionName: string) => {
      const dimension = model.getDimension(dimensionName);
      return new DimensionRef(dimension, context);
    },
    column: (columnName: string) => new ColumnRef(model, columnName, context),
  };
}

export const JOIN_WEIGHTS: Record<AnyJoin["type"], number> = {
  oneToOne: 1,
  oneToMany: 3,
  manyToOne: 2,
  manyToMany: 4,
};

export const REVERSED_JOIN: Record<AnyJoin["type"], AnyJoin["type"]> = {
  oneToOne: "oneToOne",
  oneToMany: "manyToOne",
  manyToOne: "oneToMany",
  manyToMany: "manyToMany",
};
