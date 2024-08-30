import { Dimension, Member, Metric } from "./member.js";

import { AnyBaseDialect } from "./dialect/base.js";
import { AnyModel } from "./model.js";
import { QueryMemberCache } from "./query-builder/query-plan/query-member.js";
import { AnyRepository } from "./repository.js";
import { SqlFragment } from "./sql-builder.js";
import { METRIC_REF_SUBQUERY_ALIAS } from "./util.js";

export abstract class Ref {
  public abstract render(
    repository: AnyRepository,
    queryMembers: QueryMemberCache,
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
  render(
    _repository: AnyRepository,
    queryMembers: QueryMemberCache,
    _dialect: AnyBaseDialect,
  ) {
    const dimensionQueryMember = queryMembers.get(this.dimension);
    return dimensionQueryMember.getSql();
  }
}

export class MetricRef extends Ref {
  constructor(
    readonly owner: Member,
    readonly metric: Metric,
    private readonly context: unknown,
  ) {
    super();
  }
  render(
    _repository: AnyRepository,
    _queryMembers: QueryMemberCache,
    dialect: AnyBaseDialect,
  ) {
    return SqlFragment.fromSql(
      `${dialect.asIdentifier(METRIC_REF_SUBQUERY_ALIAS)}.${dialect.asIdentifier(
        this.metric.getAlias(),
      )}`,
    );
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
  render(
    repository: AnyRepository,
    queryMembers: QueryMemberCache,
    dialect: AnyBaseDialect,
  ) {
    const { sql: asSql, bindings } = this.model.getAs(
      repository,
      queryMembers,
      dialect,
      this.context,
    );
    return SqlFragment.make({
      sql: `${asSql}.${dialect.asIdentifier(this.columnName)}`,
      bindings,
    });
  }
}

export class AliasRef<
  T extends DimensionRef | ColumnRef | MetricRef,
> extends Ref {
  constructor(
    readonly alias: string,
    readonly aliasOf: T,
  ) {
    super();
  }
  render(
    _repository: AnyRepository,
    _queryMembers: QueryMemberCache,
    dialect: AnyBaseDialect,
  ) {
    return SqlFragment.fromSql(dialect.asIdentifier(this.alias));
  }
}

export class IdentifierRef extends Ref {
  constructor(private readonly identifier: string) {
    super();
  }
  render(
    _repository: AnyRepository,
    _queryMembers: QueryMemberCache,
    dialect: AnyBaseDialect,
  ) {
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

  render(
    repository: AnyRepository,

    queryMembers: QueryMemberCache,
    dialect: AnyBaseDialect,
  ) {
    const sql: string[] = [];
    const bindings: unknown[] = [];
    for (let i = 0; i < this.strings.length; i++) {
      sql.push(this.strings[i]!);
      if (this.values[i]) {
        const value = this.values[i];
        if (value instanceof Ref) {
          const result = value.render(repository, queryMembers, dialect);
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
