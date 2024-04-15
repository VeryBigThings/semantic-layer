import {
  AggregateWith,
  DimensionWithGranularity,
  Granularity,
  GranularityByDimensionType,
  GranularityIndex,
  MemberFormat,
  MemberNameToType,
  MemberType,
  SqlWithBindings,
} from "./types.js";

import { Simplify } from "type-fest";
import { BaseDialect } from "./dialect/base.js";
import { sqlAsSqlWithBindings } from "./query-builder/util.js";

export abstract class ModelRef {
  public abstract render(
    dialect: BaseDialect,
    context: unknown,
  ): SqlWithBindings;
}

export class ColumnRef extends ModelRef {
  constructor(
    public readonly model: AnyModel,
    public readonly name: string,
  ) {
    super();
  }
  render(dialect: BaseDialect, _context: unknown) {
    const sql = `${dialect.asIdentifier(
      this.model.getAs(),
    )}.${dialect.asIdentifier(this.name)}`;
    return {
      sql,
      bindings: [],
    };
  }
}

export class IdentifierRef extends ModelRef {
  constructor(private readonly identifier: string) {
    super();
  }
  render(dialect: BaseDialect, _context: unknown) {
    return {
      sql: dialect.asIdentifier(this.identifier),
      bindings: [],
    };
  }
}

export class DimensionRef extends ModelRef {
  constructor(private readonly dimension: Dimension) {
    super();
  }
  render(dialect: BaseDialect, context: unknown) {
    return this.dimension.getSql(dialect, context);
  }
}

export class SqlWithRefs extends ModelRef {
  constructor(
    public readonly strings: string[],
    public readonly values: unknown[],
  ) {
    super();
  }
  render(dialect: BaseDialect, context: unknown) {
    const sql: string[] = [];
    const bindings: unknown[] = [];
    for (let i = 0; i < this.strings.length; i++) {
      sql.push(this.strings[i]!);
      const nextValue = this.values[i];
      if (nextValue) {
        if (nextValue instanceof ModelRef) {
          const result = nextValue.render(dialect, context);
          sql.push(result.sql);
          bindings.push(...result.bindings);
        } else {
          sql.push("?");
          bindings.push(nextValue);
        }
      }
    }
    return {
      sql: sql.join(""),
      bindings,
    };
  }
}

export type MemberSqlFn<C, DN extends string = string> = (args: {
  identifier: (name: string) => IdentifierRef;
  model: {
    column: (name: string) => ColumnRef;
    dimension: (name: DN) => DimensionRef;
  };
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlWithRefs;
  getContext: () => C;
}) => ModelRef;

export type ModelSqlFn<C> = (args: {
  identifier: (name: string) => IdentifierRef;
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlWithRefs;
  getContext: () => C;
}) => ModelRef;

function typeHasGranularity(
  type: string,
): type is keyof GranularityByDimensionType {
  return type in GranularityByDimensionType;
}

export type WithGranularityDimensions<
  N extends string,
  T extends string,
> = T extends keyof GranularityByDimensionType
  ? { [k in N]: T } & DimensionWithGranularity<N, T>
  : { [k in N]: T };

export interface DimensionProps<C, DN extends string = string> {
  type: MemberType;
  sql?: MemberSqlFn<C, DN>;
  format?: MemberFormat;
  primaryKey?: boolean;
  description?: string;
}

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type AnyDimensionProps = DimensionProps<any, string>;
export interface MetricProps<C, DN extends string = string> {
  type: MemberType;
  // TODO: allow custom aggregate functions: ({sql: SqlFn<never>, metric: MetricRef}) => SqlWithRefs
  aggregateWith: AggregateWith;
  sql?: MemberSqlFn<C, DN>;
  format?: MemberFormat;
  description?: string;
}

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type AnyMetricProps = MetricProps<any, string>;

export abstract class Member {
  public abstract readonly name: string;
  public abstract readonly model: AnyModel;
  public abstract props: AnyDimensionProps | AnyMetricProps;

  abstract getSql(
    dialect: BaseDialect,
    context: unknown,
    modelAlias?: string,
  ): SqlWithBindings;
  abstract isMetric(): this is Metric;
  abstract isDimension(): this is Dimension;

  getAlias(dialect: BaseDialect) {
    return dialect.asIdentifier(
      `${this.model.name}___${this.name.replaceAll(".", "___")}`,
    );
  }
  getPath() {
    return `${this.model.name}.${this.name}`;
  }
  renderSql(
    dialect: BaseDialect,
    context: unknown,
  ): SqlWithBindings | undefined {
    if (this.props.sql) {
      const result = this.props.sql({
        identifier: (name: string) => new IdentifierRef(name),
        model: {
          column: (name: string) => new ColumnRef(this.model, name),
          dimension: (name: string) =>
            new DimensionRef(this.model.getDimension(name)),
        },
        sql: (strings, ...values) => new SqlWithRefs([...strings], values),
        getContext: () => context,
      });
      return result.render(dialect, context);
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
}

export class Dimension extends Member {
  constructor(
    public readonly model: AnyModel,
    public readonly name: string,
    public readonly props: AnyDimensionProps,
    public readonly granularity?: Granularity,
  ) {
    super();
  }
  getSql(dialect: BaseDialect, context: unknown, modelAlias?: string) {
    if (modelAlias) {
      return sqlAsSqlWithBindings(
        `${dialect.asIdentifier(modelAlias)}.${this.getAlias(dialect)}`,
      );
    }
    const result = this.getSqlWithoutGranularity(dialect, context);

    if (this.granularity) {
      return {
        sql: dialect.withGranularity(this.granularity, result.sql),
        bindings: result.bindings,
      };
    }
    return result;
  }
  getSqlWithoutGranularity(dialect: BaseDialect, context: unknown) {
    return (
      this.renderSql(dialect, context) ??
      sqlAsSqlWithBindings(
        `${dialect.asIdentifier(this.model.getAs())}.${dialect.asIdentifier(
          this.name,
        )}`,
      )
    );
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
  getSql(dialect: BaseDialect, context: unknown, modelAlias?: string) {
    if (modelAlias) {
      return sqlAsSqlWithBindings(
        `${dialect.asIdentifier(modelAlias)}.${this.getAlias(dialect)}`,
      );
    }
    return (
      this.renderSql(dialect, context) ??
      sqlAsSqlWithBindings(
        `${dialect.asIdentifier(this.model.getAs())}.${dialect.asIdentifier(
          this.name,
        )}`,
      )
    );
  }
  getAggregateSql(dialect: BaseDialect, context: unknown, modelAlias?: string) {
    const { sql, bindings } = this.getSql(dialect, context, modelAlias);
    return {
      sql: dialect.aggregate(this.props.aggregateWith, sql),
      bindings,
    };
  }
  isDimension(): this is Dimension {
    return false;
  }
  isMetric(): this is Metric {
    return true;
  }
}

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type AnyModel<C = any> = Model<C, any, any, any>;
export type ModelConfig<C> =
  | { type: "table"; name: string }
  | { type: "sqlQuery"; alias: string; sql: ModelSqlFn<C> };

export class Model<
  C,
  N extends string,
  D extends MemberNameToType = MemberNameToType,
  M extends MemberNameToType = MemberNameToType,
> {
  public readonly dimensions: Record<string, Dimension> = {};
  public readonly metrics: Record<string, Metric> = {};

  constructor(
    public readonly name: N,
    public readonly config: ModelConfig<C>,
  ) {
    this.name = name;
  }
  withDimension<
    DN1 extends string,
    DP extends DimensionProps<C, string & keyof D>,
  >(
    name: DN1,
    dimension: DP,
  ): Model<C, N, Simplify<D & WithGranularityDimensions<DN1, DP["type"]>>, M> {
    this.dimensions[name] = new Dimension(this, name, dimension);
    if (typeHasGranularity(dimension.type)) {
      const granularity = GranularityByDimensionType[dimension.type];
      for (const g of granularity) {
        this.dimensions[`${name}.${g}`] = new Dimension(
          this,
          `${name}.${g}`,
          {
            ...dimension,
            type: GranularityIndex[g].type,
            description: GranularityIndex[g].description,
          },
          g,
        );
      }
    }
    return this;
  }
  withMetric<MN1 extends string, MP extends MetricProps<C, string & keyof D>>(
    name: MN1,
    metric: MP,
  ): Model<C, N, D, Simplify<M & { [k in MN1]: MP["type"] }>> {
    this.metrics[name] = new Metric(this, name, metric);
    return this;
  }
  getMetric(name: string & keyof M) {
    const metric = this.metrics[name];
    if (!metric) {
      throw new Error(`Metric ${name} not found in model ${this.name}`);
    }
    return metric;
  }
  getDimension(name: string & keyof D) {
    const dimension = this.dimensions[name];
    if (!dimension) {
      throw new Error(`Dimension ${name} not found in model ${this.name}`);
    }
    return dimension;
  }
  getPrimaryKeyDimensions() {
    return Object.values(this.dimensions).filter((d) => d.props.primaryKey);
  }
  getMember(name: string & (keyof D | keyof M)) {
    const member = this.dimensions[name] || this.metrics[name];
    if (!member) {
      throw new Error(`Member ${name} not found in model ${this.name}`);
    }
    return member;
  }
  getDimensions() {
    return Object.values(this.dimensions);
  }
  getMetrics() {
    return Object.values(this.metrics);
  }
  getAs() {
    return this.config.type === "sqlQuery"
      ? this.config.alias
      : this.config.name;
  }
  getSql(dialect: BaseDialect, context: C) {
    if (this.config.type === "sqlQuery") {
      const result = this.config.sql({
        identifier: (name: string) => new IdentifierRef(name),
        sql: (strings: TemplateStringsArray, ...values: unknown[]) =>
          new SqlWithRefs([...strings], values),
        getContext: () => context,
      });
      return result.render(dialect, context);
    }
    throw new Error("Model is not a SQL query");
  }
}

const VALID_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

export function model<C = undefined>() {
  return {
    withName: <N extends string>(name: N) => {
      if (!VALID_NAME_RE.test(name)) {
        throw new Error(`Invalid model name: ${name}`);
      }

      return {
        fromTable: (tableName?: string) => {
          return new Model<C, N>(name, {
            type: "table",
            name: tableName ?? name,
          });
        },
        fromSqlQuery: (sql: ModelSqlFn<C>) => {
          return new Model<C, N>(name, { type: "sqlQuery", alias: name, sql });
        },
      };
    },
  };
}
