import { Dimension, Metric } from "../member.js";
import {
  ColumnRef,
  DimensionRef,
  IdentifierRef,
  MetricAliasColumnOrDimensionRef,
  MetricAliasMetricRef,
  MetricRef,
  SqlFn,
} from "../sql-fn.js";

import { AnyBaseDialect } from "../dialect/base.js";
import { pathToAlias } from "../helpers.js";
import { AnyModel } from "../model.js";
import { QueryContext } from "../query-builder/query-plan/query-context.js";
import { MetricQueryMember } from "../query-builder/query-plan/query-member.js";
import { AnyRepository } from "../repository.js";
import { SqlFragment } from "../sql-builder.js";
import { MemberProps } from "../types.js";

export interface BasicMetricSqlFnArgs<
  C,
  DN extends string = string,
  MN extends string = string,
> {
  identifier: (name: string) => IdentifierRef;
  model: {
    column: (name: string) => MetricAliasColumnOrDimensionRef<ColumnRef>;
    dimension: (name: DN) => MetricAliasColumnOrDimensionRef<DimensionRef>;
    metric: (name: MN) => MetricAliasMetricRef;
  };
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlFn;
  getContext: () => C;
}

export type BasicMetricSqlFn<
  C,
  DN extends string = string,
  MN extends string = string,
> = (args: BasicMetricSqlFnArgs<C, DN, MN>) => SqlFn;

export type AnyBasicMetricSqlFn = BasicMetricSqlFn<any, string, string>;

export type BasicMetricProps<
  C,
  DN extends string = string,
  MN extends string = string,
> = MemberProps<{
  sql: BasicMetricSqlFn<C, DN, MN>;
}>;

export type AnyBasicMetricProps = BasicMetricProps<any, string>;

export class BasicMetric extends Metric {
  constructor(
    public readonly model: AnyModel,
    public readonly name: string,
    public readonly props: AnyBasicMetricProps,
  ) {
    super();
  }
  getAlias() {
    return `${this.model.name}___${pathToAlias(this.name)}`;
  }
  getPath() {
    return `${this.model.name}.${this.name}`;
  }
  getDescription() {
    return this.props.description;
  }
  getType() {
    return this.props.type;
  }
  getFormat() {
    return this.props.format;
  }
  isPrivate() {
    return !!this.props.private;
  }
  clone(model: AnyModel) {
    return new BasicMetric(model, this.name, { ...this.props });
  }

  getQueryMember(
    queryContext: QueryContext,
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ): BasicMetricQueryMember {
    return new BasicMetricQueryMember(
      queryContext,
      repository,
      dialect,
      context,
      this,
    );
  }

  isDimension(): this is Dimension {
    return false;
  }
  isMetric(): this is Metric {
    return true;
  }
}

export class BasicMetricQueryMember extends MetricQueryMember {
  readonly sqlFnResult: SqlFn;
  readonly sqlFnRenderResult: SqlFragment;
  constructor(
    readonly queryContext: QueryContext,
    readonly repository: AnyRepository,
    readonly dialect: AnyBaseDialect,
    readonly context: unknown,
    readonly member: BasicMetric,
  ) {
    super();
    this.sqlFnResult = this.callSqlFn();
    this.sqlFnRenderResult = this.sqlFnResult.render(
      this.repository,
      this.queryContext,
      this.dialect,
    );
  }
  private callSqlFn(): SqlFn {
    let refAliasCounter = 0;
    const getNextRefAlias = () =>
      `${this.member.model.name}___${this.member.name}___mr_${refAliasCounter++}`;

    return this.member.props.sql({
      identifier: (name: string) => new IdentifierRef(name),
      model: {
        column: (name: string) => {
          const columnRef = new ColumnRef(
            this.member.model,
            name,
            this.context,
          );
          return new MetricAliasColumnOrDimensionRef(
            getNextRefAlias(),
            columnRef,
          );
        },
        dimension: (name: string) => {
          const dimensionRef = new DimensionRef(
            this.member.model.getDimension(name),
            this.context,
          );
          return new MetricAliasColumnOrDimensionRef(
            getNextRefAlias(),
            dimensionRef,
          );
        },
        metric: (name: string) => {
          const metricRef = new MetricRef(
            this.member,
            this.member.model.getMetric(name),
            this.context,
          );
          return new MetricAliasMetricRef(getNextRefAlias(), metricRef);
        },
      },
      sql: (strings, ...values) => new SqlFn([...strings], values),
      getContext: () => this.context,
    });
  }
}
