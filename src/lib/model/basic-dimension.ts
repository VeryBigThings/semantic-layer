import { Get, Simplify } from "type-fest";
import { Dimension, Metric } from "../member.js";
import {
  QueryMember,
  QueryMemberCache,
} from "../query-builder/query-plan/query-member.js";
import {
  ColumnRef,
  DimensionRef,
  IdentifierRef,
  Ref,
  SqlFn,
} from "../sql-fn.js";
import {
  DimensionWithTemporalGranularity,
  MemberFormat,
  TemporalGranularityByDimensionType,
} from "../types.js";

import invariant from "tiny-invariant";
import { AnyBaseDialect } from "../dialect/base.js";
import { AnyModel } from "../model.js";
import { AnyRepository } from "../repository.js";
import { SqlFragment } from "../sql-builder.js";
import { isNonEmptyArray } from "../util.js";

export interface DimensionSqlFnArgs<C, DN extends string = string> {
  identifier: (name: string) => IdentifierRef;
  model: {
    column: (name: string) => ColumnRef;
    dimension: (name: DN) => DimensionRef;
  };
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlFn;
  getContext: () => C;
}

export type DimensionSqlFn<C, DN extends string = string> = (
  args: DimensionSqlFnArgs<C, DN>,
) => Ref;

export type AnyDimensionSqlFn = DimensionSqlFn<any, string>;

export type WithTemporalGranularityDimensions<
  N extends string,
  T extends string,
> = T extends keyof TemporalGranularityByDimensionType
  ? { [k in N]: T } & DimensionWithTemporalGranularity<N, T>
  : { [k in N]: T };

// TODO: Figure out how to ensure that DimensionProps and MetricProps have support for all valid member types
export type BasicDimensionProps<C, DN extends string = string> = Simplify<
  {
    sql?: DimensionSqlFn<C, DN>;
    primaryKey?: boolean;
    description?: string;
  } & (
    | { type: "string"; format?: MemberFormat<"string"> }
    | { type: "number"; format?: MemberFormat<"number"> }
    | { type: "date"; format?: MemberFormat<"date">; omitGranularity?: boolean }
    | {
        type: "datetime";
        format?: MemberFormat<"datetime">;
        omitGranularity?: boolean;
      }
    | { type: "time"; format?: MemberFormat<"time">; omitGranularity?: boolean }
    | { type: "boolean"; format?: MemberFormat<"boolean"> }
  )
>;

export type AnyBasicDimensionProps = BasicDimensionProps<any, string>;

export type DimensionHasTemporalGranularity<DP extends AnyBasicDimensionProps> =
  Get<DP, "type"> extends "datetime" | "date" | "time"
    ? Get<DP, "omitGranularity"> extends true
      ? false
      : true
    : false;

export class BasicDimension extends Dimension {
  constructor(
    public readonly model: AnyModel,
    public readonly name: string,
    public readonly props: AnyBasicDimensionProps,
  ) {
    super();
  }
  clone(model: AnyModel) {
    return new BasicDimension(model, this.name, { ...this.props });
  }

  isPrimaryKey() {
    return !!this.props.primaryKey;
  }
  isGranularity() {
    return false;
  }
  isDimension(): this is Dimension {
    return true;
  }
  isMetric(): this is Metric {
    return false;
  }

  getQueryMember(
    queryMembers: QueryMemberCache,
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ): BasicDimensionQueryMember {
    return new BasicDimensionQueryMember(
      queryMembers,
      repository,
      dialect,
      context,
      this,
    );
  }
}

export class BasicDimensionQueryMember extends QueryMember {
  private sqlFnRenderResult: SqlFragment | undefined;
  constructor(
    readonly queryMembers: QueryMemberCache,
    readonly repository: AnyRepository,
    readonly dialect: AnyBaseDialect,
    readonly context: unknown,
    readonly member: BasicDimension,
  ) {
    super();
    const sqlFnResult = this.callSqlFn();
    if (sqlFnResult) {
      this.sqlFnRenderResult = sqlFnResult.render(
        this.repository,
        this.queryMembers,
        this.dialect,
      );
    }
  }
  private callSqlFn(): Ref | undefined {
    if (this.member.props.sql) {
      return this.member.props.sql({
        identifier: (name: string) => new IdentifierRef(name),
        model: {
          column: (name: string) => {
            return new ColumnRef(this.member.model, name, this.context);
          },
          dimension: (name: string) => {
            return new DimensionRef(
              this.member.model.getDimension(name),
              this.context,
            );
          },
        },
        sql: (strings, ...values) => new SqlFn([...strings], values),
        getContext: () => this.context,
      });
    }
  }
  getAlias() {
    return this.member.getAlias();
  }
  getSql() {
    if (this.sqlFnRenderResult) {
      return this.sqlFnRenderResult;
    }

    const { sql: asSql, bindings } = this.member.model.getAs(
      this.repository,
      this.queryMembers,
      this.dialect,
      this.context,
    );
    const sql = `${asSql}.${this.dialect.asIdentifier(this.member.name)}`;

    return SqlFragment.make({ sql, bindings });
  }
  getFilterSql() {
    return this.getSql();
  }
  getModelQueryProjection() {
    const { sql, bindings } = this.getSql();
    const fragment = this.dialect.fragment(
      `${sql} as ${this.dialect.asIdentifier(this.member.getAlias())}`,
      bindings,
    );
    return [fragment];
  }
  getSegmentQueryProjection(modelQueryAlias: string) {
    const fragment = this.dialect.fragment(
      `${this.dialect.asIdentifier(modelQueryAlias)}.${this.dialect.asIdentifier(
        this.member.getAlias(),
      )} as ${this.dialect.asIdentifier(this.member.getAlias())}`,
    );
    return [fragment];
  }
  getSegmentQueryGroupBy(modelQueryAlias: string) {
    const fragment = this.dialect.fragment(
      `${this.dialect.asIdentifier(modelQueryAlias)}.${this.dialect.asIdentifier(
        this.member.getAlias(),
      )}`,
    );
    return [fragment];
  }
  getRootQueryProjection(segmentQueryAlias: string) {
    const fragment = this.dialect.fragment(
      `${this.dialect.asIdentifier(segmentQueryAlias)}.${this.dialect.asIdentifier(
        this.member.getAlias(),
      )} as ${this.dialect.asIdentifier(this.member.getAlias())}`,
    );
    return [fragment];
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
