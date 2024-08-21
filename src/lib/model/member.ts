import { Get, Simplify } from "type-fest";
import {
  AliasRef,
  ColumnRef,
  DimensionRef,
  IdentifierRef,
  Ref,
  SqlFn,
} from "../sql-fn.js";
import {
  DimensionWithTemporalGranularity,
  MemberFormat,
  NextColumnRefOrDimensionRefAlias,
  TemporalGranularity,
  TemporalGranularityByDimensionType,
} from "../types.js";

import { AnyBaseDialect } from "../dialect/base.js";
import { AnyModel } from "../model.js";
import { AnyRepository } from "../repository.js";
import { SqlFragment } from "../sql-builder.js";

export interface MemberSqlFnArgs<C, DN extends string = string> {
  identifier: (name: string) => IdentifierRef;
  model: {
    column: (name: string) => ColumnRef | AliasRef;
    dimension: (name: DN) => DimensionRef | AliasRef;
  };
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlFn;
  getContext: () => C;
}

export type MemberSqlFn<C, DN extends string = string> = (
  args: MemberSqlFnArgs<C, DN>,
) => Ref;

export type MetricSqlFn<C, DN extends string = string> = (
  args: MemberSqlFnArgs<C, DN>,
) => SqlFn;

export type WithTemporalGranularityDimensions<
  N extends string,
  T extends string,
> = T extends keyof TemporalGranularityByDimensionType
  ? { [k in N]: T } & DimensionWithTemporalGranularity<N, T>
  : { [k in N]: T };

// TODO: Figure out how to ensure that DimensionProps and MetricProps have support for all valid member types
export type DimensionProps<C, DN extends string = string> = Simplify<
  {
    sql?: MemberSqlFn<C, DN>;
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

export type AnyDimensionProps = DimensionProps<any, string>;

export type DimensionHasTemporalGranularity<DP extends AnyDimensionProps> = Get<
  DP,
  "type"
> extends "datetime" | "date" | "time"
  ? Get<DP, "omitGranularity"> extends true
    ? false
    : true
  : false;

// TODO: Figure out how to ensure that DimensionProps and MetricProps have support for all valid member types
export type MetricProps<C, DN extends string = string> = Simplify<
  {
    sql?: MemberSqlFn<C, DN>;
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
export type AnyMetricProps = MetricProps<any, string>;

export abstract class Member {
  public abstract readonly name: string;
  public abstract readonly model: AnyModel;
  public abstract props: AnyDimensionProps | AnyMetricProps;

  abstract getSql(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ): SqlFragment;
  abstract isMetric(): this is Metric;
  abstract isDimension(): this is Dimension;

  getQuotedAlias(dialect: AnyBaseDialect) {
    return dialect.asIdentifier(this.getAlias());
  }
  getAlias() {
    return `${this.model.name}___${this.name.replaceAll(".", "___")}`;
  }
  getPath() {
    return `${this.model.name}.${this.name}`;
  }
  callSqlFn(
    context: unknown,
    nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ) {
    if (this.props.sql) {
      return this.props.sql({
        identifier: (name: string) => new IdentifierRef(name),
        model: {
          column: (name: string) => {
            const columnRef = new ColumnRef(this.model, name, context);
            if (nextColumnRefOrDimensionRefAlias) {
              return new AliasRef(
                nextColumnRefOrDimensionRefAlias(),
                columnRef,
              );
            }
            return columnRef;
          },
          dimension: (name: string) => {
            const dimensionRef = new DimensionRef(
              this.model.getDimension(name),
              context,
            );
            if (nextColumnRefOrDimensionRefAlias) {
              return new AliasRef(
                nextColumnRefOrDimensionRefAlias(),
                dimensionRef,
              );
            }
            return dimensionRef;
          },
        },
        sql: (strings, ...values) => new SqlFn([...strings], values),
        getContext: () => context,
      });
    }
  }
  renderSql(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
    nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ): SqlFragment | undefined {
    const result = this.callSqlFn(context, nextColumnRefOrDimensionRefAlias);
    if (result) {
      return result.render(repository, dialect);
    }
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
  unsafeFormatValue(value: unknown) {
    const format = this.getFormat();
    if (typeof format === "function") {
      return (format as (value: unknown) => string)(value);
    }
    if (format === "currency") {
      return `$${value}`;
    }
    if (format === "percentage") {
      return `${value}%`;
    }
    return String(value);
  }
  abstract clone(model: AnyModel): Member;
}

export class Dimension extends Member {
  constructor(
    public readonly model: AnyModel,
    public readonly name: string,
    public readonly props: AnyDimensionProps,
    public readonly granularity?: TemporalGranularity,
  ) {
    super();
  }
  clone(model: AnyModel) {
    return new Dimension(model, this.name, { ...this.props }, this.granularity);
  }
  getSql(repository: AnyRepository, dialect: AnyBaseDialect, context: unknown) {
    const result = this.getSqlWithoutGranularity(repository, dialect, context);

    if (this.granularity) {
      return SqlFragment.make({
        sql: dialect.withGranularity(this.granularity, result.sql),
        bindings: result.bindings,
      });
    }
    return result;
  }
  getSqlWithoutGranularity(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ) {
    const result = this.renderSql(repository, dialect, context);

    if (result) {
      return result;
    }

    const { sql: asSql, bindings } = this.model.getAs(
      repository,
      dialect,
      context,
    );
    const sql = `${asSql}.${dialect.asIdentifier(this.name)}`;

    return SqlFragment.make({ sql, bindings });
  }
  getGranularity() {
    return this.granularity;
  }
  isGranularity() {
    return !!this.granularity;
  }
  isPrimaryKey() {
    return !!this.props.primaryKey;
  }
  isDimension(): this is Dimension {
    return true;
  }
  isMetric(): this is Metric {
    return false;
  }
}

export class Metric extends Member {
  constructor(
    public readonly model: AnyModel,
    public readonly name: string,
    public readonly props: AnyMetricProps,
  ) {
    super();
  }
  clone(model: AnyModel) {
    return new Metric(model, this.name, { ...this.props });
  }
  getNextColumnRefOrDimensionRefAlias() {
    let columnRefOrDimensionRefAliasCounter = 0;
    return () =>
      `${this.name}___metric_ref_${columnRefOrDimensionRefAliasCounter++}`;
  }

  getSql(repository: AnyRepository, dialect: AnyBaseDialect, context: unknown) {
    const result = this.renderSql(
      repository,
      dialect,
      context,
      this.getNextColumnRefOrDimensionRefAlias(),
    );

    if (result) {
      return result;
    }

    const { sql: asSql, bindings } = this.model.getAs(
      repository,
      dialect,
      context,
    );
    const sql = `${asSql}.${dialect.asIdentifier(this.name)}`;

    return SqlFragment.make({
      sql,
      bindings,
    });
  }

  getRefsSqls(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ) {
    const sqlFnResult = this.callSqlFn(context);

    if (sqlFnResult instanceof SqlFn) {
      const nextColumnRefOrDimensionRefAlias =
        this.getNextColumnRefOrDimensionRefAlias();

      const columnOrDimensionRefs: SqlFragment[] = [];
      const valuesQueue = [...sqlFnResult.values];

      while (valuesQueue.length > 0) {
        const value = valuesQueue.shift()!;
        if (value instanceof DimensionRef || value instanceof ColumnRef) {
          const alias = nextColumnRefOrDimensionRefAlias();
          const { sql, bindings } = value.render(repository, dialect);

          columnOrDimensionRefs.push(
            SqlFragment.make({
              sql: `${sql} as ${dialect.asIdentifier(alias)}`,
              bindings,
            }),
          );
        } else if (value instanceof SqlFn) {
          valuesQueue.push(...value.values);
        }
      }
      return columnOrDimensionRefs;
    }
  }

  isDimension(): this is Dimension {
    return false;
  }
  isMetric(): this is Metric {
    return true;
  }
}
