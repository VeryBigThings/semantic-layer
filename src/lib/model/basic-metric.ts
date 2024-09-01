import { Dimension, Metric } from "../member.js";
import {
  QueryMember,
  QueryMemberCache,
} from "../query-builder/query-plan/query-member.js";
import {
  ColumnRef,
  DimensionRef,
  IdentifierRef,
  MetricAliasRef,
  MetricRef,
  SqlFn,
} from "../sql-fn.js";

import invariant from "tiny-invariant";
import { AnyBaseDialect } from "../dialect/base.js";
import { AnyModel } from "../model.js";
import { AnyRepository } from "../repository.js";
import { SqlFragment } from "../sql-builder.js";
import { MemberProps } from "../types.js";
import { isNonEmptyArray } from "../util.js";

export interface MetricSqlFnArgs<
  C,
  DN extends string = string,
  MN extends string = string,
> {
  identifier: (name: string) => IdentifierRef;
  model: {
    column: (name: string) => MetricAliasRef<ColumnRef>;
    dimension: (name: DN) => MetricAliasRef<DimensionRef>;
    metric: (name: MN) => MetricAliasRef<MetricRef>;
  };
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlFn;
  getContext: () => C;
}

export type AnyMetricSqlFn = MetricSqlFn<any, string, string>;

export type MetricSqlFn<
  C,
  DN extends string = string,
  MN extends string = string,
> = (args: MetricSqlFnArgs<C, DN, MN>) => SqlFn;

export type BasicMetricProps<
  C,
  DN extends string = string,
  MN extends string = string,
> = MemberProps<{
  sql: MetricSqlFn<C, DN, MN>;
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
  clone(model: AnyModel) {
    return new BasicMetric(model, this.name, { ...this.props });
  }

  getQueryMember(
    queryMembers: QueryMemberCache,
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ): BasicMetricQueryMember {
    return new BasicMetricQueryMember(
      queryMembers,
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

export class BasicMetricQueryMember extends QueryMember {
  private sqlFnResult: SqlFn;
  private sqlFnRenderResult: SqlFragment;
  constructor(
    readonly queryMembers: QueryMemberCache,
    readonly repository: AnyRepository,
    readonly dialect: AnyBaseDialect,
    readonly context: unknown,
    readonly member: BasicMetric,
  ) {
    super();
    this.sqlFnResult = this.callSqlFn();
    this.sqlFnRenderResult = this.sqlFnResult.render(
      this.repository,
      this.queryMembers,
      this.dialect,
    );
  }
  private callSqlFn(): SqlFn {
    let refAliasCounter = 0;
    const getNextRefAlias = () =>
      `${this.member.name}___metric_ref_${refAliasCounter++}`;

    return this.member.props.sql({
      identifier: (name: string) => new IdentifierRef(name),
      model: {
        column: (name: string) => {
          const columnRef = new ColumnRef(
            this.member.model,
            name,
            this.context,
          );
          return new MetricAliasRef(getNextRefAlias(), columnRef);
        },
        dimension: (name: string) => {
          const dimensionRef = new DimensionRef(
            this.member.model.getDimension(name),
            this.context,
          );
          return new MetricAliasRef(getNextRefAlias(), dimensionRef);
        },
        metric: (name: string) => {
          const metricRef = new MetricRef(
            this.member,
            this.member.model.getMetric(name),
            this.context,
          );
          return new MetricAliasRef(getNextRefAlias(), metricRef);
        },
      },
      sql: (strings, ...values) => new SqlFn([...strings], values),
      getContext: () => this.context,
    });
  }

  getAlias() {
    return this.member.getAlias();
  }
  getSql() {
    return this.sqlFnRenderResult;
  }
  getFilterSql() {
    return SqlFragment.fromSql(this.dialect.asIdentifier(this.getAlias()));
  }
  getModelQueryProjection() {
    const sqlFnResult = this.sqlFnResult;
    const filterFn = (ref: unknown): ref is MetricAliasRef<any> =>
      ref instanceof MetricAliasRef;
    return sqlFnResult.filterRefs(filterFn).map(({ alias, aliasOf }) => {
      const { sql, bindings } = aliasOf.render(
        this.repository,
        this.queryMembers,
        this.dialect,
      );
      return SqlFragment.make({
        sql: `${sql} as ${this.dialect.asIdentifier(alias)}`,
        bindings,
      });
    });
  }
  getSegmentQueryProjection(_modelQueryAlias: string) {
    const { sql, bindings } = this.getSql();
    const fragment = this.dialect.fragment(
      `${sql} as ${this.dialect.asIdentifier(this.member.getAlias())}`,
      bindings,
    );
    return [fragment];
  }
  getSegmentQueryGroupBy(_modelQueryAlias: string) {
    return [];
  }
  getRootQueryProjection(segmentQueryAlias: string) {
    const fragment = this.dialect.fragment(
      `${this.dialect.asIdentifier(segmentQueryAlias)}.${this.dialect.asIdentifier(
        this.member.getAlias(),
      )} as ${this.dialect.asIdentifier(this.member.getAlias())}`,
    );
    return [fragment];
  }
  getMetricRefs() {
    const filterFn = (ref: unknown): ref is MetricAliasRef<MetricRef> =>
      ref instanceof MetricAliasRef && ref.aliasOf instanceof MetricRef;
    return this.sqlFnResult.filterRefs(filterFn).map((v) => v.aliasOf);
  }

  getReferencedModels() {
    const referencedModels = [this.member.model.name];
    invariant(
      isNonEmptyArray(referencedModels),
      `Referenced models not found for ${this.member.getPath()}`,
    );
    return referencedModels;
  }
}
