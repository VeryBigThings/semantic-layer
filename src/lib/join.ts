import invariant from "tiny-invariant";
import type { BaseDialect } from "./dialect/base.js";
import { AnyModel } from "./model.js";
import type { AnyRepository } from "./repository.js";
import { SqlWithBindings } from "./types.js";

export abstract class JoinRef {
  public abstract render(
    repository: AnyRepository,
    dialect: BaseDialect,
  ): SqlWithBindings;
}

export class JoinDimensionRef<
  N extends string,
  DN extends string,
> extends JoinRef {
  constructor(
    private readonly model: N,
    private readonly dimension: DN,
    private readonly context: unknown,
  ) {
    super();
  }
  render(repository: AnyRepository, dialect: BaseDialect) {
    return repository
      .getModel(this.model)
      .getDimension(this.dimension)
      .getSql(dialect, this.context);
  }
}

export class JoinColumnRef<N extends string> extends JoinRef {
  constructor(
    private readonly model: N,
    private readonly column: string,
  ) {
    super();
  }
  render(repository: AnyRepository, dialect: BaseDialect) {
    const model = repository.getModel(this.model);
    return {
      sql: `${dialect.asIdentifier(model.getAs())}.${dialect.asIdentifier(
        this.column,
      )}`,
      bindings: [],
    };
  }
}

export class JoinIdentifierRef extends JoinRef {
  constructor(private readonly identifier: string) {
    super();
  }
  render(_repository: AnyRepository, dialect: BaseDialect) {
    return {
      sql: dialect.asIdentifier(this.identifier),
      bindings: [],
    };
  }
}

export function makeModelJoinPayload(model: AnyModel, context: unknown) {
  return {
    dimension: (name: string) => {
      const dimension = model.getDimension(name);
      invariant(
        dimension,
        `Dimension ${name} not found in model ${model.name}`,
      );
      return new JoinDimensionRef(model.name, name, context);
    },
    column: (name: string) => new JoinColumnRef(model.name, name),
  };
}

export class JoinOnDef {
  constructor(
    private readonly strings: string[],
    private readonly values: unknown[],
  ) {}
  render(repository: AnyRepository, dialect: BaseDialect) {
    const sql: string[] = [];
    const bindings: unknown[] = [];
    for (let i = 0; i < this.strings.length; i++) {
      sql.push(this.strings[i]!);
      if (this.values[i]) {
        const value = this.values[i];
        if (value instanceof JoinRef) {
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

export interface Join<C> {
  left: string;
  right: string;
  joinOnDef: (context: C) => JoinOnDef;
  reversed: boolean;
  type: "oneToOne" | "oneToMany" | "manyToOne" | "manyToMany";
}
// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type AnyJoin = Join<any>;

export type JoinFn<
  C,
  DN extends string,
  N1 extends string,
  N2 extends string,
> = (args: {
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => JoinOnDef;
  models: JoinDimensions<DN, N1, N2>;
  identifier: (name: string) => JoinIdentifierRef;
  getContext: () => C;
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
    dimension: (
      name: ModelDimensionsWithoutModelPrefix<N1, DN>,
    ) => JoinDimensionRef<TK, ModelDimensionsWithoutModelPrefix<N1, DN>>;
    column: (name: string) => JoinColumnRef<TK>;
  };
} & {
  [TK in N2]: {
    dimension: (
      name: ModelDimensionsWithoutModelPrefix<N2, DN>,
    ) => JoinDimensionRef<TK, ModelDimensionsWithoutModelPrefix<N2, DN>>;
    column: (name: string) => JoinColumnRef<TK>;
  };
};

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
