import {
  Granularity,
  GranularityByDimensionType,
  SqlWithBindings,
} from "../../types.js";

import { BaseDialect } from "../dialect/base.js";
import { sqlAsSqlWithBindings } from "../query/util.js";

export abstract class Ref {
  public abstract render(dialect: BaseDialect): SqlWithBindings;
}

export class ColumnRef extends Ref {
  constructor(
    public readonly table: AnyTable,
    public readonly name: string,
  ) {
    super();
  }
  render(dialect: BaseDialect) {
    const sql = `${dialect.asIdentifier(
      this.table.name,
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

export type SqlDef = ColumnRef | SqlWithRefs;

export type SqlFn<DN extends string = string> = (args: {
  table: {
    column: (name: string) => ColumnRef;
    dimension: (name: DN) => DimensionRef;
  };
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlWithRefs;
}) => SqlDef;

function typeHasGranularity(
  type: string,
): type is keyof GranularityByDimensionType {
  return type in GranularityByDimensionType;
}

export type WithGranularityDimensionNames<
  N extends string,
  T extends string,
> = T extends keyof GranularityByDimensionType
  ? N | `${N}.${GranularityByDimensionType[T][number]}`
  : N;

export type DimensionType =
  | "string"
  | "number"
  | "date"
  | "time"
  | "datetime"
  | "boolean";

export interface DimensionProps<DN extends string = string> {
  type: DimensionType;
  sql?: SqlFn<DN>;
  primaryKey?: boolean;
}

export type MetricType = "count" | "sum" | "avg" | "min" | "max";
export interface MetricProps<DN extends string = string> {
  type: MetricType;
  sql?: SqlFn<DN>;
}

export abstract class Member {
  public abstract readonly name: string;
  public abstract readonly table: AnyTable;
  public abstract props: { sql?: SqlFn };

  abstract getSql(dialect: BaseDialect, tableAlias?: string): SqlWithBindings;
  abstract isMetric(): this is Metric;
  abstract isDimension(): this is Dimension;

  getAlias(dialect: BaseDialect) {
    return dialect.asIdentifier(
      `${this.table.name}___${this.name.replaceAll(".", "___")}`,
    );
  }
  getPath() {
    return `${this.table.name}.${this.name}`;
  }
  renderSql(dialect: BaseDialect): SqlWithBindings | undefined {
    if (this.props.sql) {
      const result = this.props.sql({
        table: {
          column: (name: string) => new ColumnRef(this.table, name),
          dimension: (name: string) =>
            new DimensionRef(this.table.getDimension(name)),
        },
        sql: (strings, ...values) => new SqlWithRefs([...strings], values),
      });
      return result.render(dialect);
    }
  }
}

export class Dimension extends Member {
  constructor(
    public readonly table: AnyTable,
    public readonly name: string,
    public readonly props: DimensionProps,
    public readonly granularity?: Granularity,
  ) {
    super();
  }
  getSql(dialect: BaseDialect, tableAlias?: string) {
    if (tableAlias) {
      return sqlAsSqlWithBindings(
        `${dialect.asIdentifier(tableAlias)}.${this.getAlias(dialect)}`,
      );
    }
    const result =
      this.renderSql(dialect) ??
      sqlAsSqlWithBindings(
        `${dialect.asIdentifier(this.table.name)}.${dialect.asIdentifier(
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
    public readonly table: AnyTable,
    public readonly name: string,
    public readonly props: MetricProps,
  ) {
    super();
  }
  getSql(dialect: BaseDialect, tableAlias?: string) {
    if (tableAlias) {
      return sqlAsSqlWithBindings(
        `${dialect.asIdentifier(tableAlias)}.${this.getAlias(dialect)}`,
      );
    }
    return (
      this.renderSql(dialect) ??
      sqlAsSqlWithBindings(
        `${dialect.asIdentifier(this.table.name)}.${dialect.asIdentifier(
          this.name,
        )}`,
      )
    );
  }
  getAggregateSql(dialect: BaseDialect, tableAlias?: string) {
    const { sql, bindings } = this.getSql(dialect, tableAlias);
    return {
      sql: `${this.props.type.toUpperCase()}(${sql})`,
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
export type AnyTable = Table<any, any, any>;
export class Table<
  TN extends string,
  DN extends string = never,
  MN extends string = never,
> {
  public readonly name: TN;
  public readonly dimensions: Record<string, Dimension> = {};
  public readonly metrics: Record<string, Metric> = {};

  constructor(name: TN) {
    this.name = name;
  }
  withDimension<DN1 extends string, DP extends DimensionProps<DN>>(
    name: DN1,
    dimension: DP,
  ): Table<TN, DN | WithGranularityDimensionNames<DN1, DP["type"]>, MN> {
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
  withMetric<MN1 extends string>(
    name: MN1,
    metric: MetricProps<DN>,
  ): Table<TN, DN, MN | MN1> {
    this.metrics[name] = new Metric(this, name, metric);
    return this;
  }
  getMetric(name: MN) {
    const metric = this.metrics[name];
    if (!metric) {
      throw new Error(`Metric ${name} not found in table ${this.name}`);
    }
    return metric;
  }
  getDimension(name: DN) {
    const dimension = this.dimensions[name];
    if (!dimension) {
      throw new Error(`Dimension ${name} not found in table ${this.name}`);
    }
    return dimension;
  }
  getPrimaryKeyDimensions() {
    return Object.values(this.dimensions).filter((d) => d.props.primaryKey);
  }
  getMember(name: DN | MN) {
    const member = this.dimensions[name] || this.metrics[name];
    if (!member) {
      throw new Error(`Member ${name} not found in table ${this.name}`);
    }
    return member;
  }
}

export function table<TN extends string>(name: TN): Table<TN> {
  return new Table(name);
}
