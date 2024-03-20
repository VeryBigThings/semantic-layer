import {
  Granularity,
  GranularityByDimensionType,
  MemberFormat,
  MemberNameToType,
  MemberType,
  SqlWithBindings,
} from "./types.js";

import { BaseDialect } from "./dialect/base.js";
import { sqlAsSqlWithBindings } from "./query-builder/util.js";

export abstract class Ref {
  public abstract render(dialect: BaseDialect): SqlWithBindings;
}

export class ColumnRef extends Ref {
  constructor(
    public readonly model: AnyModel,
    public readonly name: string,
  ) {
    super();
  }
  render(dialect: BaseDialect) {
    const sql = `${dialect.asIdentifier(
      this.model.getAs(),
    )}.${dialect.asIdentifier(this.name)}`;
    return {
      sql,
      bindings: [],
    };
  }
}

export class DimensionRef extends Ref {
  constructor(private readonly dimension: Dimension) {
    super();
  }
  render(dialect: BaseDialect) {
    return this.dimension.getSql(dialect);
  }
}

export class SqlWithRefs extends Ref {
  constructor(
    public readonly strings: string[],
    public readonly values: unknown[],
  ) {
    super();
  }
  render(dialect: BaseDialect) {
    const sql: string[] = [];
    const bindings: unknown[] = [];
    for (let i = 0; i < this.strings.length; i++) {
      sql.push(this.strings[i]!);
      const nextValue = this.values[i];
      if (nextValue) {
        if (nextValue instanceof Ref) {
          const result = nextValue.render(dialect);
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

export type SqlFn<DN extends string = string> = (args: {
  model: {
    column: (name: string) => ColumnRef;
    dimension: (name: DN) => DimensionRef;
  };
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlWithRefs;
}) => Ref;

function typeHasGranularity(
  type: string,
): type is keyof GranularityByDimensionType {
  return type in GranularityByDimensionType;
}

export type WithGranularityDimensions<
  N extends string,
  T extends string,
> = T extends keyof GranularityByDimensionType
  ? { [k in N]: T } & {
      [k in `${N}.${GranularityByDimensionType[T][number]}`]: number;
    }
  : { [k in N]: T };

export interface DimensionProps<DN extends string = string> {
  type: MemberType;
  sql?: SqlFn<DN>;
  format?: MemberFormat;
  primaryKey?: boolean;
  description?: string;
}

export type MetricType = "count" | "sum" | "avg" | "min" | "max";
export interface MetricProps<DN extends string = string> {
  type: MemberType;
  // TODO: allow custom aggregate functions: ({sql: SqlFn<never>, metric: MetricRef}) => SqlWithRefs
  aggregateWith: MetricType;
  sql?: SqlFn<DN>;
  format?: MemberFormat;
  description?: string;
}

export abstract class Member {
  public abstract readonly name: string;
  public abstract readonly model: AnyModel;
  public abstract props: DimensionProps | MetricProps;

  abstract getSql(dialect: BaseDialect, modelAlias?: string): SqlWithBindings;
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
  renderSql(dialect: BaseDialect): SqlWithBindings | undefined {
    if (this.props.sql) {
      const result = this.props.sql({
        model: {
          column: (name: string) => new ColumnRef(this.model, name),
          dimension: (name: string) =>
            new DimensionRef(this.model.getDimension(name)),
        },
        sql: (strings, ...values) => new SqlWithRefs([...strings], values),
      });
      return result.render(dialect);
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
    public readonly props: DimensionProps,
    public readonly granularity?: Granularity,
  ) {
    super();
  }
  getSql(dialect: BaseDialect, modelAlias?: string) {
    if (modelAlias) {
      return sqlAsSqlWithBindings(
        `${dialect.asIdentifier(modelAlias)}.${this.getAlias(dialect)}`,
      );
    }
    const result =
      this.renderSql(dialect) ??
      sqlAsSqlWithBindings(
        `${dialect.asIdentifier(this.model.getAs())}.${dialect.asIdentifier(
          this.name,
        )}`,
      );
    if (this.granularity) {
      return {
        sql: dialect.withGranularity(this.granularity, result.sql),
        bindings: result.bindings,
      };
    }
    return result;
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
    public readonly props: MetricProps,
  ) {
    super();
  }
  getSql(dialect: BaseDialect, modelAlias?: string) {
    if (modelAlias) {
      return sqlAsSqlWithBindings(
        `${dialect.asIdentifier(modelAlias)}.${this.getAlias(dialect)}`,
      );
    }
    return (
      this.renderSql(dialect) ??
      sqlAsSqlWithBindings(
        `${dialect.asIdentifier(this.model.getAs())}.${dialect.asIdentifier(
          this.name,
        )}`,
      )
    );
  }
  getAggregateSql(dialect: BaseDialect, modelAlias?: string) {
    const { sql, bindings } = this.getSql(dialect, modelAlias);
    return {
      sql: `${this.props.aggregateWith.toUpperCase()}(${sql})`,
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
export type AnyModel = Model<any, any, any>;
export type ModelConfig =
  | { type: "table"; name: string }
  | { type: "sqlQuery"; alias: string; sql: string };

export class Model<
  N extends string,
  D extends MemberNameToType = MemberNameToType,
  M extends MemberNameToType = MemberNameToType,
> {
  public readonly dimensions: Record<string, Dimension> = {};
  public readonly metrics: Record<string, Metric> = {};

  constructor(
    public readonly name: N,
    public readonly config: ModelConfig,
  ) {
    this.name = name;
  }
  withDimension<
    DN1 extends string,
    DP extends DimensionProps<string & keyof D>,
  >(
    name: DN1,
    dimension: DP,
  ): Model<N, D & WithGranularityDimensions<DN1, DP["type"]>, M> {
    this.dimensions[name] = new Dimension(this, name, dimension);
    if (typeHasGranularity(dimension.type)) {
      const granularity = GranularityByDimensionType[dimension.type];
      for (const g of granularity) {
        this.dimensions[`${name}.${g}`] = new Dimension(
          this,
          `${name}.${g}`,
          { ...dimension, type: "number" },
          g,
        );
      }
    }
    return this;
  }
  withMetric<MN1 extends string, MP extends MetricProps<string & keyof D>>(
    name: MN1,
    metric: MP,
  ): Model<N, D, M & { [k in MN1]: MP["type"] }> {
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
}

const VALID_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

export function model<N extends string>(name: N) {
  if (!VALID_NAME_RE.test(name)) {
    throw new Error(`Invalid model name: ${name}`);
  }

  return {
    fromTable: (tableName: string) => {
      return new Model<N>(name, { type: "table", name: tableName });
    },
    fromSqlQuery: (sql: string) => {
      return new Model<N>(name, { type: "sqlQuery", alias: name, sql });
    },
  };
}
