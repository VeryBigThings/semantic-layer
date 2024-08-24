import { Get, Simplify } from "type-fest";
import {
  AliasRef,
  ColumnRef,
  DimensionRef,
  IdentifierRef,
  MetricRef,
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
import { pathToAlias } from "../helpers.js";
import { AnyModel } from "../model.js";
import { AnyRepository } from "../repository.js";
import { SqlFragment } from "../sql-builder.js";

export interface DimensionSqlFnArgs<C, DN extends string = string> {
  identifier: (name: string) => IdentifierRef;
  model: {
    column: (name: string) => ColumnRef | AliasRef;
    dimension: (name: DN) => DimensionRef | AliasRef;
  };
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlFn;
  getContext: () => C;
}

export interface MetricSqlFnArgs<
  C,
  DN extends string = string,
  MN extends string = string,
> {
  identifier: (name: string) => IdentifierRef;
  model: {
    column: (name: string) => ColumnRef | AliasRef;
    dimension: (name: DN) => DimensionRef | AliasRef;
    metric: (name: MN) => MetricRef | AliasRef;
  };
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlFn;
  getContext: () => C;
}

export type AnyMetricSqlFn = MetricSqlFn<any, string, string>;

export type DimensionSqlFn<C, DN extends string = string> = (
  args: DimensionSqlFnArgs<C, DN>,
) => Ref;

export type MetricSqlFn<
  C,
  DN extends string = string,
  MN extends string = string,
> = (args: MetricSqlFnArgs<C, DN, MN>) => SqlFn;

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

// TODO: Figure out how to ensure that DimensionProps and MetricProps have support for all valid member types
export type BasicMetricProps<
  C,
  DN extends string = string,
  MN extends string = string,
> = Simplify<
  {
    sql?: MetricSqlFn<C, DN, MN>;
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

function callSqlFn(
  member: Member,
  context: unknown,
  nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
): Ref | undefined {
  if (member.props.sql) {
    const dimensionMemberModelProp = {
      column: (name: string) => {
        const columnRef = new ColumnRef(member.model, name, context);
        if (nextColumnRefOrDimensionRefAlias) {
          return new AliasRef(nextColumnRefOrDimensionRefAlias(), columnRef);
        }
        return columnRef;
      },
      dimension: (name: string) => {
        const dimensionRef = new DimensionRef(
          member.model.getDimension(name),
          context,
        );
        if (nextColumnRefOrDimensionRefAlias) {
          return new AliasRef(nextColumnRefOrDimensionRefAlias(), dimensionRef);
        }
        return dimensionRef;
      },
    };
    if (member.isDimension()) {
      return member.props.sql({
        identifier: (name: string) => new IdentifierRef(name),
        model: dimensionMemberModelProp,
        sql: (strings, ...values) => new SqlFn([...strings], values),
        getContext: () => context,
      });
    }
    if (member.isMetric()) {
      return member.props.sql({
        identifier: (name: string) => new IdentifierRef(name),
        model: {
          ...dimensionMemberModelProp,
          metric: (name: string) => {
            const metricRef = new MetricRef(
              member,
              member.model.getMetric(name),
              context,
            );
            if (nextColumnRefOrDimensionRefAlias) {
              return new AliasRef(
                nextColumnRefOrDimensionRefAlias(),
                metricRef,
              );
            }
            return metricRef;
          },
        },
        sql: (strings, ...values) => new SqlFn([...strings], values),
        getContext: () => context,
      });
    }
  }
}

function callAndRenderSqlFn(
  repository: AnyRepository,
  dialect: AnyBaseDialect,
  member: Member,
  context: unknown,
  nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
) {
  const result = callSqlFn(member, context, nextColumnRefOrDimensionRefAlias);
  if (result) {
    return result.render(repository, dialect);
  }
}

export abstract class Member {
  public abstract readonly name: string;
  public abstract readonly model: AnyModel;
  public abstract props: AnyBasicDimensionProps | AnyBasicMetricProps;

  abstract getSql(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ): SqlFragment;
  abstract isMetric(): this is BasicMetric;
  abstract isDimension(): this is BasicDimension;

  abstract getModelQueryProjection(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ): SqlFragment[];
  abstract getSegmentQueryProjection(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
    modelQueryAlias: string,
  ): SqlFragment[];
  abstract getSegmentQueryGroupBy(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
    modelQueryAlias: string,
  ): SqlFragment[];
  abstract getRootQueryProjection(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
    segmentQueryAlias: string,
  ): SqlFragment[];

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

export class BasicDimension extends Member {
  constructor(
    public readonly model: AnyModel,
    public readonly name: string,
    public readonly props: AnyBasicDimensionProps,
    public readonly granularity?: TemporalGranularity,
  ) {
    super();
  }
  clone(model: AnyModel) {
    return new BasicDimension(
      model,
      this.name,
      { ...this.props },
      this.granularity,
    );
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
    const result = callAndRenderSqlFn(repository, dialect, this, context);

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
  isDimension(): this is BasicDimension {
    return true;
  }
  isMetric(): this is BasicMetric {
    return false;
  }
  getModelQueryProjection(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ) {
    const { sql, bindings } = this.getSql(repository, dialect, context);
    const fragment = dialect.fragment(
      `${sql} as ${dialect.asIdentifier(this.getAlias())}`,
      bindings,
    );
    return [fragment];
  }

  getSegmentQueryProjection(
    _repository: AnyRepository,
    dialect: AnyBaseDialect,
    _context: unknown,
    modelQueryAlias: string,
  ) {
    const fragment = dialect.fragment(
      `${dialect.asIdentifier(modelQueryAlias)}.${dialect.asIdentifier(
        this.getAlias(),
      )} as ${dialect.asIdentifier(this.getAlias())}`,
    );
    return [fragment];
  }
  getSegmentQueryGroupBy(
    _repository: AnyRepository,
    dialect: AnyBaseDialect,
    _context: unknown,
    modelQueryAlias: string,
  ): SqlFragment[] {
    const fragment = dialect.fragment(
      `${dialect.asIdentifier(modelQueryAlias)}.${dialect.asIdentifier(
        this.getAlias(),
      )}`,
    );
    return [fragment];
  }
  getRootQueryProjection(
    _repository: AnyRepository,
    dialect: AnyBaseDialect,
    _context: unknown,
    segmentQueryAlias: string,
  ) {
    const fragment = dialect.fragment(
      `${dialect.asIdentifier(segmentQueryAlias)}.${dialect.asIdentifier(
        this.getAlias(),
      )} as ${dialect.asIdentifier(this.getAlias())}`,
    );
    return [fragment];
  }
}

export class BasicMetric extends Member {
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
  getNextColumnRefOrDimensionRefAlias() {
    let columnRefOrDimensionRefAliasCounter = 0;
    return () =>
      `${this.name}___metric_ref_${columnRefOrDimensionRefAliasCounter++}`;
  }

  getSql(repository: AnyRepository, dialect: AnyBaseDialect, context: unknown) {
    const result = callAndRenderSqlFn(
      repository,
      dialect,
      this,
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

  getModelQueryProjection(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ) {
    const sqlFnResult = callSqlFn(this, context);

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
        } else if (value instanceof MetricRef) {
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
    return [];
  }

  getSegmentQueryProjection(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
    _modelQueryAlias: string,
  ): SqlFragment[] {
    const { sql, bindings } = this.getSql(repository, dialect, context);
    const fragment = dialect.fragment(
      `${sql} as ${dialect.asIdentifier(this.getAlias())}`,
      bindings,
    );
    return [fragment];
  }
  getSegmentQueryGroupBy(
    _repository: AnyRepository,
    _dialect: AnyBaseDialect,
    _context: unknown,
    _modelQueryAlias: string,
  ): SqlFragment[] {
    return [];
  }

  getRootQueryProjection(
    _repository: AnyRepository,
    dialect: AnyBaseDialect,
    _context: unknown,
    segmentQueryAlias: string,
  ) {
    const fragment = dialect.fragment(
      `${dialect.asIdentifier(segmentQueryAlias)}.${dialect.asIdentifier(
        this.getAlias(),
      )} as ${dialect.asIdentifier(this.getAlias())}`,
    );
    return [fragment];
  }

  getMetricRefs(context: unknown) {
    const sqlFnResult = callSqlFn(this, context);
    if (sqlFnResult) {
      const valuesToProcess: unknown[] = [sqlFnResult];
      const refs: MetricRef[] = [];

      while (valuesToProcess.length > 0) {
        const ref = valuesToProcess.pop()!;
        if (ref instanceof MetricRef) {
          refs.push(ref);
        }
        if (ref instanceof SqlFn) {
          valuesToProcess.push(...ref.values);
        }
      }
      return refs;
    }
    return [];
  }

  isDimension(): this is BasicDimension {
    return false;
  }
  isMetric(): this is BasicMetric {
    return true;
  }
}
