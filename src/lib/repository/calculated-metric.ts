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
import { MemberProps, ModelMemberWithoutModelPrefix } from "../types.js";

import { AnyBaseDialect } from "../dialect/base.js";
import { pathToAlias } from "../helpers.js";
import { QueryContext } from "../query-builder/query-plan/query-context.js";
import { MetricQueryMember } from "../query-builder/query-plan/query-member.js";
import { AnyRepository } from "../repository.js";
import { SqlFragment } from "../sql-builder.js";

export type CalculatedMetricSqlFnArgsModels<
  TModelNames extends string,
  TDimensionNames extends string,
  TMetricNames extends string,
> = {
  [TK in TModelNames]: {
    metric: (
      metricName: ModelMemberWithoutModelPrefix<TK, TMetricNames>,
    ) => MetricAliasMetricRef;
    dimension: (
      dimensionName: ModelMemberWithoutModelPrefix<TK, TDimensionNames>,
    ) => MetricAliasColumnOrDimensionRef<DimensionRef>;
    column: (columnName: string) => MetricAliasColumnOrDimensionRef<ColumnRef>;
  };
};

export interface CalculatedMetricSqlFnArgs<
  TContext,
  TModelNames extends string,
  TDimensionNames extends string,
  TMetricNames extends string,
> {
  identifier: (name: string) => IdentifierRef;
  models: CalculatedMetricSqlFnArgsModels<
    TModelNames,
    TDimensionNames,
    TMetricNames
  >;
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlFn;
  getContext: () => TContext;
}

export type CalculatedMetricSqlFn<
  TContext,
  TModelNames extends string,
  TDimensionNames extends string,
  TMetricNames extends string,
> = (
  args: CalculatedMetricSqlFnArgs<
    TContext,
    TModelNames,
    TDimensionNames,
    TMetricNames
  >,
) => SqlFn;

export type AnyCalculatedMetricSqlFn = CalculatedMetricSqlFn<
  any,
  any,
  any,
  any
>;

export type CalculatedMetricProps<
  TContext,
  TModelNames extends string,
  TDimensionNames extends string,
  TMetricNames extends string,
> = MemberProps<{
  sql: CalculatedMetricSqlFn<
    TContext,
    TModelNames,
    TDimensionNames,
    TMetricNames
  >;
}>;

export type AnyCalculatedMetricProps = CalculatedMetricProps<
  any,
  any,
  any,
  any
>;

export class CalculatedMetric extends Metric {
  constructor(
    public readonly path: string,
    public readonly props: AnyCalculatedMetricProps,
  ) {
    super();
  }
  getAlias() {
    return pathToAlias(this.path);
  }
  getPath() {
    return this.path;
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

  getQueryMember(
    queryContext: QueryContext,
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ): CalculatedMetricQueryMember {
    return new CalculatedMetricQueryMember(
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

export class CalculatedMetricQueryMember extends MetricQueryMember {
  readonly sqlFnResult: SqlFn;
  readonly sqlFnRenderResult: SqlFragment;
  constructor(
    readonly queryContext: QueryContext,
    readonly repository: AnyRepository,
    readonly dialect: AnyBaseDialect,
    readonly context: unknown,
    readonly member: CalculatedMetric,
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
    const models = this.repository.getModels();
    const getNextRefAlias = () =>
      `${this.member.getAlias()}___metric_ref_${refAliasCounter++}`;

    return this.member.props.sql({
      identifier: (name: string) => new IdentifierRef(name),
      models: models.reduce<
        CalculatedMetricSqlFnArgsModels<string, string, string>
      >((acc, model) => {
        acc[model.name] = {
          metric: (name: string) => {
            const metricRef = new MetricRef(
              this.member,
              model.getMetric(name),
              this.context,
            );
            return new MetricAliasMetricRef(getNextRefAlias(), metricRef);
          },
          dimension: (name: string) => {
            const dimensionRef = new DimensionRef(
              model.getDimension(name),
              this.context,
            );
            return new MetricAliasColumnOrDimensionRef(
              getNextRefAlias(),
              dimensionRef,
            );
          },
          column: (name: string) => {
            const columnRef = new ColumnRef(model, name, this.context);
            return new MetricAliasColumnOrDimensionRef(
              getNextRefAlias(),
              columnRef,
            );
          },
        };
        return acc;
      }, {}),
      sql: (strings, ...values) => new SqlFn([...strings], values),
      getContext: () => this.context,
    });
  }
}
