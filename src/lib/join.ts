import type { BaseDialect } from "./dialect/base.js";
import type { Repository } from "./repository.js";

export class JoinDimensionRef<N extends string, DN extends string> {
  constructor(
    private readonly model: N,
    private readonly dimension: DN,
  ) {}
  render(repository: Repository, dialect: BaseDialect) {
    return repository
      .getModel(this.model)
      .getDimension(this.dimension)
      .getSql(dialect);
  }
}
export class JoinOnDef {
  constructor(
    private readonly strings: string[],
    private readonly values: unknown[],
  ) {}
  render(repository: Repository, dialect: BaseDialect) {
    const sql: string[] = [];
    const bindings: unknown[] = [];
    for (let i = 0; i < this.strings.length; i++) {
      sql.push(this.strings[i]!);
      if (this.values[i]) {
        const value = this.values[i];
        if (value instanceof JoinDimensionRef) {
          const result = value.render(repository, dialect);
          sql.push(result.sql);
          bindings.push(...result.bindings);
        } else {
          sql.push("?");
          bindings.push(value);
        }
      }
    }
    return {
      sql: sql.join(""),
      bindings,
    };
  }
}

export interface Join {
  left: string;
  right: string;
  joinOnDef: JoinOnDef;
  reversed: boolean;
  type: "oneToOne" | "oneToMany" | "manyToOne" | "manyToMany";
}

export type JoinFn<
  DN extends string,
  N1 extends string,
  N2 extends string,
> = (args: {
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => JoinOnDef;
  dimensions: JoinDimensions<DN, N1, N2>;
}) => JoinOnDef;

export type ModelDimensionsWithoutModelPrefix<
  N extends string,
  DN extends string,
> = DN extends `${N}.${infer D}` ? D : never;

export type JoinDimensions<
  DN extends string,
  N1 extends string,
  N2 extends string,
> = {
  [TK in N1]: {
    [DK in ModelDimensionsWithoutModelPrefix<N1, DN>]: JoinDimensionRef<TK, DK>;
  };
} & {
  [TK in N2]: {
    [DK in ModelDimensionsWithoutModelPrefix<N2, DN>]: JoinDimensionRef<TK, DK>;
  };
};

export const JOIN_WEIGHTS: Record<Join["type"], number> = {
  oneToOne: 1,
  oneToMany: 3,
  manyToOne: 2,
  manyToMany: 4,
};

export const REVERSED_JOIN: Record<Join["type"], Join["type"]> = {
  oneToOne: "oneToOne",
  oneToMany: "manyToOne",
  manyToOne: "oneToMany",
  manyToMany: "manyToMany",
};
