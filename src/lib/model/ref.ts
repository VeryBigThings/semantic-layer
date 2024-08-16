import { AnyBaseDialect } from "../dialect/base.js";
import { AnyModel } from "../model.js";
import { SqlFragment } from "../sql-builder.js";
import { Dimension } from "./member.js";

export type NextColumnRefOrDimensionRefAlias = () => string;

export abstract class ModelRef {
  public abstract render(
    dialect: AnyBaseDialect,
    context: unknown,
    nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ): SqlFragment;
}

export class ColumnRef extends ModelRef {
  constructor(
    public readonly model: AnyModel,
    public readonly name: string,
  ) {
    super();
  }
  render(
    dialect: AnyBaseDialect,
    context: unknown,
    nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ) {
    if (nextColumnRefOrDimensionRefAlias) {
      return SqlFragment.make({
        sql: nextColumnRefOrDimensionRefAlias(),
        bindings: [],
      });
    }
    const { sql: asSql, bindings } = this.model.getAs(dialect, context);
    const sql = `${asSql}.${dialect.asIdentifier(this.name)}`;
    return SqlFragment.make({
      sql,
      bindings,
    });
  }
}

export class IdentifierRef extends ModelRef {
  constructor(private readonly identifier: string) {
    super();
  }
  render(
    dialect: AnyBaseDialect,
    _context: unknown,
    _nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ) {
    return SqlFragment.make({
      sql: dialect.asIdentifier(this.identifier),
      bindings: [],
    });
  }
}

export class DimensionRef extends ModelRef {
  constructor(private readonly dimension: Dimension) {
    super();
  }
  render(
    dialect: AnyBaseDialect,
    context: unknown,
    nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ) {
    if (nextColumnRefOrDimensionRefAlias) {
      return SqlFragment.make({
        sql: nextColumnRefOrDimensionRefAlias(),
        bindings: [],
      });
    }
    return this.dimension.getSql(dialect, context);
  }
}

export class SqlWithRefs extends ModelRef {
  constructor(
    public readonly strings: string[],
    public readonly values: unknown[],
  ) {
    super();
  }
  render(
    dialect: AnyBaseDialect,
    context: unknown,
    nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ) {
    const sql: string[] = [];
    const bindings: unknown[] = [];
    for (let i = 0; i < this.strings.length; i++) {
      sql.push(this.strings[i]!);
      const nextValue = this.values[i];
      if (nextValue) {
        if (nextValue instanceof ModelRef) {
          const result = nextValue.render(
            dialect,
            context,
            nextColumnRefOrDimensionRefAlias,
          );
          sql.push(result.sql);
          bindings.push(...result.bindings);
        } else {
          sql.push("?");
          bindings.push(nextValue);
        }
      }
    }
    return SqlFragment.make({
      sql: sql.join(""),
      bindings,
    });
  }
  getRefsSqls(
    dialect: AnyBaseDialect,
    context: unknown,
    nextColumnRefOrDimensionRefAlias: NextColumnRefOrDimensionRefAlias,
  ) {
    const columnOrDimensionRefs: SqlFragment[] = [];
    for (let i = 0; i < this.values.length; i++) {
      const value = this.values[i];
      if (value instanceof DimensionRef || value instanceof ColumnRef) {
        const alias = nextColumnRefOrDimensionRefAlias();
        const { sql, bindings } = value.render(dialect, context);

        columnOrDimensionRefs.push(
          SqlFragment.make({
            sql: `${sql} as ${alias}`,
            bindings,
          }),
        );
      } else if (value instanceof SqlWithRefs) {
        columnOrDimensionRefs.push(
          ...value.getRefsSqls(
            dialect,
            context,
            nextColumnRefOrDimensionRefAlias,
          ),
        );
      }
    }
    return columnOrDimensionRefs;
  }
}
