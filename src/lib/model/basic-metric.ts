import { Dimension, Metric } from "../member.js";
import {
  QueryMember,
  QueryMemberCache,
} from "../query-builder/query-plan/query-member.js";
import {
  AliasRef,
  ColumnRef,
  DimensionRef,
  IdentifierRef,
  MetricRef,
  SqlFn,
} from "../sql-fn.js";

import { Simplify } from "type-fest";
import { AnyBaseDialect } from "../dialect/base.js";
import { AnyModel } from "../model.js";
import { AnyRepository } from "../repository.js";
import { SqlFragment } from "../sql-builder.js";
import { MemberFormat } from "../types.js";

export interface MetricSqlFnArgs<
  C,
  DN extends string = string,
  MN extends string = string,
> {
  identifier: (name: string) => IdentifierRef;
  model: {
    column: (name: string) => AliasRef<ColumnRef>;
    dimension: (name: DN) => AliasRef<DimensionRef>;
    metric: (name: MN) => AliasRef<MetricRef>;
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

// TODO: Figure out how to ensure that DimensionProps and MetricProps have support for all valid member types
export type BasicMetricProps<
  C,
  DN extends string = string,
  MN extends string = string,
> = Simplify<
  {
    sql: MetricSqlFn<C, DN, MN>;
    description?: string;
  } & (
    | { type: "string"; format?: MemberFormat<"string"> }
    | { type: "number"; format?: MemberFormat<"number"> }
    | { type: "date"; format?: MemberFormat<"date"> }
    | { type: "datetime"; format?: MemberFormat<"datetime"> }
    | { type: "time"; format?: MemberFormat<"time"> }
    | { type: "boolean"; format?: MemberFormat<"boolean"> }
  )
>;
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
          return new AliasRef(getNextRefAlias(), columnRef);
        },
        dimension: (name: string) => {
          const dimensionRef = new DimensionRef(
            this.member.model.getDimension(name),
            this.context,
          );
          return new AliasRef(getNextRefAlias(), dimensionRef);
        },
        metric: (name: string) => {
          const metricRef = new MetricRef(
            this.member,
            this.member.model.getMetric(name),
            this.context,
          );
          return new AliasRef(getNextRefAlias(), metricRef);
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
    const result = this.sqlFnRenderResult;

    if (result) {
      return result;
    }

    const { sql: asSql, bindings } = this.member.model.getAs(
      this.repository,
      this.queryMembers,
      this.dialect,
      this.context,
    );
    const sql = `${asSql}.${this.dialect.asIdentifier(this.member.name)}`;

    return SqlFragment.make({
      sql,
      bindings,
    });
  }
  getFilterSql() {
    return SqlFragment.fromSql(this.dialect.asIdentifier(this.getAlias()));
  }
  getModelQueryProjection() {
    const sqlFnResult = this.sqlFnResult;

    const refs: SqlFragment[] = [];
    const valuesQueue = [...sqlFnResult.values];

    while (valuesQueue.length > 0) {
      const value = valuesQueue.shift()!;
      if (value instanceof AliasRef) {
        const alias = value.alias;
        const { sql, bindings } = value.aliasOf.render(
          this.repository,
          this.queryMembers,
          this.dialect,
        );

        refs.push(
          SqlFragment.make({
            sql: `${sql} as ${this.dialect.asIdentifier(alias)}`,
            bindings,
          }),
        );
      } else if (value instanceof SqlFn) {
        valuesQueue.push(...value.values);
      }
    }
    return refs;
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
    const sqlFnResult = this.sqlFnResult;
    if (sqlFnResult) {
      const valuesToProcess: unknown[] = [sqlFnResult];
      const refs: MetricRef[] = [];

      while (valuesToProcess.length > 0) {
        const ref = valuesToProcess.pop()!;
        if (ref instanceof AliasRef && ref.aliasOf instanceof MetricRef) {
          refs.push(ref.aliasOf);
        }
        if (ref instanceof SqlFn) {
          valuesToProcess.push(...ref.values);
        }
      }
      return refs;
    }
    return [];
  }
}
