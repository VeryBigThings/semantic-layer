import { Dimension, Member, Metric } from "./member.js";

import { AnyBaseDialect } from "./dialect/base.js";
import { AnyModel } from "./model.js";
import { QueryContext } from "./query-builder/query-plan/query-context.js";
import { AnyRepository } from "./repository.js";
import { SqlFragment } from "./sql-builder.js";
import { METRIC_REF_SUBQUERY_ALIAS } from "./util.js";

export function valueIsMetricAliasRef(
  value: unknown,
): value is AnyMetricAliasColumnOrDimensionRef | MetricAliasMetricRef {
  return (
    value instanceof MetricAliasColumnOrDimensionRef ||
    value instanceof MetricAliasMetricRef
  );
}

export abstract class Ref {
  public abstract render(
    repository: AnyRepository,
    queryContext: QueryContext,
    dialect: AnyBaseDialect,
  ): SqlFragment;
}

export class DimensionRef extends Ref {
  constructor(
    readonly member: Dimension,
    private readonly context: unknown,
  ) {
    super();
  }
  render(
    _repository: AnyRepository,
    queryContext: QueryContext,
    _dialect: AnyBaseDialect,
  ) {
    const dimensionQueryMember = queryContext.getQueryMember(this.member);
    return dimensionQueryMember.getSql();
  }
}

export class MetricRef extends Ref {
  constructor(
    readonly owner: Member,
    readonly member: Metric,
    private readonly context: unknown,
  ) {
    super();
  }
  render(
    _repository: AnyRepository,
    _queryContext: QueryContext,
    dialect: AnyBaseDialect,
  ) {
    return SqlFragment.fromSql(
      `${dialect.asIdentifier(METRIC_REF_SUBQUERY_ALIAS)}.${dialect.asIdentifier(
        this.member.getAlias(),
      )}`,
    );
  }
}

export class ColumnRef extends Ref {
  constructor(
    readonly model: AnyModel,
    private readonly columnName: string,
    private readonly context: unknown,
  ) {
    super();
  }
  render(
    repository: AnyRepository,
    queryContext: QueryContext,
    dialect: AnyBaseDialect,
  ) {
    const { sql: asSql, bindings } = this.model.getAs(
      repository,
      queryContext,
      dialect,
      this.context,
    );
    return SqlFragment.make({
      sql: `${asSql}.${dialect.asIdentifier(this.columnName)}`,
      bindings,
    });
  }
}

export class MetricAliasColumnOrDimensionRef<
  T extends DimensionRef | ColumnRef,
> extends Ref {
  private isGroupedBy = false;
  constructor(
    readonly alias: string,
    readonly aliasOf: T,
  ) {
    super();
  }
  groupBy() {
    this.isGroupedBy = true;
    return this;
  }
  getIsGroupedBy() {
    return this.isGroupedBy;
  }
  render(
    _repository: AnyRepository,
    _queryContext: QueryContext,
    dialect: AnyBaseDialect,
  ) {
    return SqlFragment.fromSql(dialect.asIdentifier(this.alias));
  }
}

export type AnyMetricAliasColumnOrDimensionRef =
  MetricAliasColumnOrDimensionRef<any>;

export class MetricAliasMetricRef extends Ref {
  private isAggregated = false;
  constructor(
    readonly alias: string,
    readonly aliasOf: MetricRef,
  ) {
    super();
  }
  aggregated() {
    this.isAggregated = true;
    return this;
  }
  getIsAggregated() {
    return this.isAggregated;
  }
  render(
    _repository: AnyRepository,
    _queryContext: QueryContext,
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
    _queryContext: QueryContext,
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

    queryContext: QueryContext,
    dialect: AnyBaseDialect,
  ) {
    const sql: string[] = [];
    const bindings: unknown[] = [];
    for (let i = 0; i < this.strings.length; i++) {
      sql.push(this.strings[i]!);
      if (this.values[i]) {
        const value = this.values[i];
        if (value instanceof Ref) {
          const result = value.render(repository, queryContext, dialect);
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
  filterRefs<T extends Ref>(predicate: (value: unknown) => value is T) {
    const refs: T[] = [];
    const valuesToProcess: unknown[] = [this];

    while (valuesToProcess.length > 0) {
      const value = valuesToProcess.pop()!;
      if (predicate(value)) {
        refs.push(value);
      }
      if (value instanceof SqlFn) {
        valuesToProcess.push(...value.values);
      }
    }

    return refs;
  }
}

export type sqlFn = (
  strings: TemplateStringsArray,
  ...values: unknown[]
) => SqlFn;
