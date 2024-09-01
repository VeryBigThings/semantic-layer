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
  MemberProps,
  TemporalGranularityByDimensionType,
} from "../types.js";

import invariant from "tiny-invariant";
import { Get } from "type-fest";
import { AnyBaseDialect } from "../dialect/base.js";
import { pathToAlias } from "../helpers.js";
import { AnyModel } from "../model.js";
import { AnyRepository } from "../repository.js";
import { SqlFragment } from "../sql-builder.js";
import { isNonEmptyArray } from "../util.js";

export interface BasicDimensionSqlFnArgs<C, DN extends string = string> {
  identifier: (name: string) => IdentifierRef;
  model: {
    column: (name: string) => ColumnRef;
    dimension: (name: DN) => DimensionRef;
  };
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlFn;
  getContext: () => C;
}

export type BasicDimensionSqlFn<C, DN extends string = string> = (
  args: BasicDimensionSqlFnArgs<C, DN>,
) => Ref;

export type AnyBasicDimensionSqlFn = BasicDimensionSqlFn<any, string>;

export type WithTemporalGranularityDimensions<
  N extends string,
  T extends string,
> = T extends keyof TemporalGranularityByDimensionType
  ? { [k in N]: T } & DimensionWithTemporalGranularity<N, T>
  : { [k in N]: T };

// TODO: Figure out how to ensure that DimensionProps and MetricProps have support for all valid member types
export type BasicDimensionProps<C, DN extends string = string> = MemberProps<
  {
    sql?: BasicDimensionSqlFn<C, DN>;
    primaryKey?: boolean;
  },
  {
    date: { omitGranularity?: boolean };
    datetime: { omitGranularity?: boolean };
    time: { omitGranularity?: boolean };
  }
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
