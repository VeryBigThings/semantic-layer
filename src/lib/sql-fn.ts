import { AnyBaseDialect } from "./dialect/base.js";
import { AnyModel } from "./model.js";
import { Dimension } from "./model/member.js";
import { AnyRepository } from "./repository.js";
import { SqlFragment } from "./sql-builder.js";

export abstract class Ref {
  public abstract render(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
  ): SqlFragment;
}

export class DimensionRef extends Ref {
  constructor(
    private readonly dimension: Dimension,
    private readonly context: unknown,
  ) {
    super();
  }
  render(repository: AnyRepository, dialect: AnyBaseDialect) {
    return this.dimension.getSql(repository, dialect, this.context);
  }
}

export class ColumnRef extends Ref {
  constructor(
    private readonly model: AnyModel,
    private readonly columnName: string,
    private readonly context: unknown,
  ) {
    super();
  }
  render(repository: AnyRepository, dialect: AnyBaseDialect) {
    const { sql: asSql, bindings } = this.model.getAs(
      repository,
      dialect,
      this.context,
    );
    return SqlFragment.make({
      sql: `${asSql}.${dialect.asIdentifier(this.columnName)}`,
      bindings,
    });
  }
}

export class AliasRef extends Ref {
  constructor(
    private readonly alias: string,
    readonly aliasOf: DimensionRef | ColumnRef,
  ) {
    super();
  }
  render(_repository: AnyRepository, dialect: AnyBaseDialect) {
    return SqlFragment.fromSql(dialect.asIdentifier(this.alias));
  }
}

export class IdentifierRef extends Ref {
  constructor(private readonly identifier: string) {
    super();
  }
  render(_repository: AnyRepository, dialect: AnyBaseDialect) {
    return SqlFragment.make({
      sql: dialect.asIdentifier(this.identifier),
      bindings: [],
    });
  }
}

export class SqlFn extends Ref {
  constructor(
    public readonly strings: string[],
    public readonly values: unknown[],
  ) {
    super();
  }

  render(repository: AnyRepository, dialect: AnyBaseDialect) {
    const sql: string[] = [];
    const bindings: unknown[] = [];
    for (let i = 0; i < this.strings.length; i++) {
      sql.push(this.strings[i]!);
      if (this.values[i]) {
        const value = this.values[i];
        if (value instanceof Ref) {
          const result = value.render(repository, dialect);
          sql.push(result.sql);
          bindings.push(...result.bindings);
        } else {
          sql.push("?");
          bindings.push(value);
        }
      }
    }

    return SqlFragment.make({
      sql: sql.join(""),
      bindings,
    });
  }
  clone(valueProcessorFn?: (value: unknown) => unknown) {
    return new SqlFn(
      [...this.strings],
      valueProcessorFn ? this.values.map(valueProcessorFn) : [...this.values],
    );
  }
}

export type sqlFn = (
  strings: TemplateStringsArray,
  ...values: unknown[]
) => SqlFn;
