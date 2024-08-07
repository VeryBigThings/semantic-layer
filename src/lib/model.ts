import {
  AnyCustomGranularityElement,
  makeCustomGranularityElementInitMaker,
} from "./custom-granularity.js";
import {
  DimensionWithTemporalGranularity,
  GranularityType,
  MemberFormat,
  MemberNameToType,
  SqlWithBindings,
  TemporalGranularity,
  TemporalGranularityByDimensionType,
  TemporalGranularityIndex,
  makeTemporalGranularityElementsForDimension,
} from "./types.js";
import { Get, Simplify } from "type-fest";

import { AnyBaseDialect } from "./dialect/base.js";
import invariant from "tiny-invariant";

export type NextColumnRefOrDimensionRefAlias = () => string;

export interface MemberSqlFnArgs<C, DN extends string = string> {
  identifier: (name: string) => IdentifierRef;
  model: {
    column: (name: string) => ColumnRef;
    dimension: (name: DN) => DimensionRef;
  };
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlWithRefs;
  getContext: () => C;
}

export type MemberSqlFn<C, DN extends string = string> = (
  args: MemberSqlFnArgs<C, DN>,
) => ModelRef;

export type MetricSqlFn<C, DN extends string = string> = (
  args: MemberSqlFnArgs<C, DN>,
) => SqlWithRefs;

export type ModelSqlFn<C> = (args: {
  identifier: (name: string) => IdentifierRef;
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlWithRefs;
  getContext: () => C;
}) => ModelRef;

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

export type AnyModel<C = any> = Model<C, any, any, any, any>;
export type ModelConfig<C> =
  | { type: "table"; name: string | ModelSqlFn<C> }
  | { type: "sqlQuery"; alias: string; sql: ModelSqlFn<C> };

export abstract class ModelRef {
  public abstract render(
    dialect: AnyBaseDialect,
    context: unknown,
    nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ): SqlWithBindings;
}

export class ColumnRef extends ModelRef {
  constructor(
    public readonly model: AnyModel,
    public readonly name: string,
  ) {
    super();
  }
  render(
    dialect: AnyBaseDialect,
    context: unknown,
    nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ) {
    if (nextColumnRefOrDimensionRefAlias) {
      return {
        sql: nextColumnRefOrDimensionRefAlias(),
        bindings: [],
      };
    }
    const { sql: asSql, bindings } = this.model.getAs(dialect, context);
    const sql = `${asSql}.${dialect.asIdentifier(this.name)}`;
    return {
      sql,
      bindings,
    };
  }
}

export class IdentifierRef extends ModelRef {
  constructor(private readonly identifier: string) {
    super();
  }
  render(
    dialect: AnyBaseDialect,
    _context: unknown,
    _nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ) {
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
  render(
    dialect: AnyBaseDialect,
    context: unknown,
    nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ) {
    if (nextColumnRefOrDimensionRefAlias) {
      return {
        sql: nextColumnRefOrDimensionRefAlias(),
        bindings: [],
      };
    }
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
  render(
    dialect: AnyBaseDialect,
    context: unknown,
    nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ) {
    const sql: string[] = [];
    const bindings: unknown[] = [];
    for (let i = 0; i < this.strings.length; i++) {
      sql.push(this.strings[i]!);
      const nextValue = this.values[i];
      if (nextValue) {
        if (nextValue instanceof ModelRef) {
          const result = nextValue.render(
            dialect,
            context,
            nextColumnRefOrDimensionRefAlias,
          );
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
  getRefsSqls(
    dialect: AnyBaseDialect,
    context: unknown,
    nextColumnRefOrDimensionRefAlias: NextColumnRefOrDimensionRefAlias,
  ) {
    const columnOrDimensionRefs: SqlWithBindings[] = [];
    for (let i = 0; i < this.values.length; i++) {
      const value = this.values[i];
      if (value instanceof DimensionRef || value instanceof ColumnRef) {
        const alias = nextColumnRefOrDimensionRefAlias();
        const { sql, bindings } = value.render(dialect, context);

        columnOrDimensionRefs.push({
          sql: `${sql} as ${alias}`,
          bindings,
        });
      } else if (value instanceof SqlWithRefs) {
        columnOrDimensionRefs.push(
          ...value.getRefsSqls(
            dialect,
            context,
            nextColumnRefOrDimensionRefAlias,
          ),
        );
      }
    }
    return columnOrDimensionRefs;
  }
}

function typeHasGranularity(
  type: string,
): type is keyof TemporalGranularityByDimensionType {
  return type in TemporalGranularityByDimensionType;
}

export abstract class Member {
  public abstract readonly name: string;
  public abstract readonly model: AnyModel;
  public abstract props: AnyDimensionProps | AnyMetricProps;

  abstract getSql(dialect: AnyBaseDialect, context: unknown): SqlWithBindings;
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
  callSqlFn(context: unknown) {
    if (this.props.sql) {
      return this.props.sql({
        identifier: (name: string) => new IdentifierRef(name),
        model: {
          column: (name: string) => new ColumnRef(this.model, name),
          dimension: (name: string) =>
            new DimensionRef(this.model.getDimension(name)),
        },
        sql: (strings, ...values) => new SqlWithRefs([...strings], values),
        getContext: () => context,
      });
    }
  }
  renderSql(
    dialect: AnyBaseDialect,
    context: unknown,
    nextColumnRefOrDimensionRefAlias?: NextColumnRefOrDimensionRefAlias,
  ): SqlWithBindings | undefined {
    const result = this.callSqlFn(context);
    if (result) {
      return result.render(dialect, context, nextColumnRefOrDimensionRefAlias);
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
  getSql(dialect: AnyBaseDialect, context: unknown) {
    const result = this.getSqlWithoutGranularity(dialect, context);

    if (this.granularity) {
      return {
        sql: dialect.withGranularity(this.granularity, result.sql),
        bindings: result.bindings,
      };
    }
    return result;
  }
  getSqlWithoutGranularity(dialect: AnyBaseDialect, context: unknown) {
    const result = this.renderSql(dialect, context);

    if (result) {
      return result;
    }

    const { sql: asSql, bindings } = this.model.getAs(dialect, context);
    const sql = `${asSql}.${dialect.asIdentifier(this.name)}`;

    return { sql, bindings };
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
  getNextColumnRefOrDimensionRefAlias(dialect: AnyBaseDialect) {
    let columnRefOrDimensionRefAliasCounter = 0;
    return () =>
      dialect.asIdentifier(
        `${this.name}___metric_ref_${columnRefOrDimensionRefAliasCounter++}`,
      );
  }

  getSql(dialect: AnyBaseDialect, context: unknown) {
    const result = this.renderSql(
      dialect,
      context,
      this.getNextColumnRefOrDimensionRefAlias(dialect),
    );

    if (result) {
      return result;
    }

    const { sql: asSql, bindings } = this.model.getAs(dialect, context);
    const sql = `${asSql}.${dialect.asIdentifier(this.name)}`;

    return {
      sql,
      bindings,
    };
  }

  getRefsSqls(dialect: AnyBaseDialect, context: unknown) {
    const sqlFnResult = this.callSqlFn(context);

    if (sqlFnResult && sqlFnResult instanceof SqlWithRefs) {
      return sqlFnResult.getRefsSqls(
        dialect,
        context,
        this.getNextColumnRefOrDimensionRefAlias(dialect),
      );
    }
  }

  isDimension(): this is Dimension {
    return false;
  }
  isMetric(): this is Metric {
    return true;
  }
}

export class Model<
  C,
  N extends string,
  D extends MemberNameToType = MemberNameToType,
  M extends MemberNameToType = MemberNameToType,
  G extends string = never,
> {
  public readonly dimensions: Record<string, Dimension> = {};
  public readonly metrics: Record<string, Metric> = {};
  public readonly categoricalGranularities: {
    name: string;
    elements: AnyCustomGranularityElement[];
  }[] = [];
  public readonly temporalGranularities: {
    name: string;
    elements: AnyCustomGranularityElement[];
  }[] = [];
  public readonly granularitiesNames: Set<string> = new Set();

  constructor(
    public readonly name: N,
    public readonly config: ModelConfig<C>,
  ) {}
  withDimension<
    DN1 extends string,
    DP extends DimensionProps<C, string & keyof D>,
    DG extends boolean = DimensionHasTemporalGranularity<DP>,
  >(
    name: Exclude<DN1, keyof D | keyof M>,
    dimension: DP,
  ): Model<
    C,
    N,
    DG extends true
      ? D & WithTemporalGranularityDimensions<DN1, DP["type"]>
      : D & { [k in DN1]: DP["type"] },
    M,
    DG extends true ? G | DN1 : G
  > {
    invariant(
      !(this.dimensions[name] || this.metrics[name]),
      `Member "${name}" already exists`,
    );

    this.dimensions[name] = new Dimension(this, name, dimension);
    if (
      typeHasGranularity(dimension.type) &&
      dimension.omitGranularity !== true
    ) {
      const granularityDimensions =
        TemporalGranularityByDimensionType[dimension.type];
      for (const g of granularityDimensions) {
        const { format: _format, ...dimensionWithoutFormat } = dimension;
        this.dimensions[`${name}.${g}`] = new Dimension(
          this,
          `${name}.${g}`,
          {
            ...dimensionWithoutFormat,
            type: TemporalGranularityIndex[g].type,
            description: TemporalGranularityIndex[g].description,
            format: (value: unknown) => `${value}`,
          },
          g,
        );
      }
      this.unsafeWithGranularity(
        name,
        makeTemporalGranularityElementsForDimension(name, dimension.type),
        "temporal",
      );
    }
    return this;
  }
  withMetric<MN1 extends string, MP extends MetricProps<C, string & keyof D>>(
    name: Exclude<MN1, keyof M | keyof D>,
    metric: MP,
  ): Model<C, N, D, M & { [k in MN1]: MP["type"] }, G> {
    invariant(
      !(this.dimensions[name] || this.metrics[name]),
      `Member "${name}" already exists`,
    );

    this.metrics[name] = new Metric(this, name, metric);
    return this;
  }
  unsafeWithGranularity(
    granularityName: string,
    elements: AnyCustomGranularityElement[],
    type: GranularityType,
  ) {
    invariant(
      this.granularitiesNames.has(granularityName) === false,
      `Granularity ${granularityName} already exists`,
    );
    this.granularitiesNames.add(granularityName);
    if (type === "categorical") {
      this.categoricalGranularities.push({ name: granularityName, elements });
    } else if (type === "temporal") {
      this.temporalGranularities.push({ name: granularityName, elements });
    }
    return this;
  }
  withCategoricalGranularity<GN extends string>(
    granularityName: Exclude<GN, G>,
    builder: (args: {
      element: ReturnType<typeof makeCustomGranularityElementInitMaker<D>>;
    }) => [AnyCustomGranularityElement, ...AnyCustomGranularityElement[]],
  ): Model<C, N, D, M, G | GN> {
    const elements = builder({
      element: makeCustomGranularityElementInitMaker(),
    });
    return this.unsafeWithGranularity(granularityName, elements, "categorical");
  }
  withTemporalGranularity<GN extends string>(
    granularityName: Exclude<GN, G>,
    builder: (args: {
      element: ReturnType<typeof makeCustomGranularityElementInitMaker<D>>;
    }) => [AnyCustomGranularityElement, ...AnyCustomGranularityElement[]],
  ): Model<C, N, D, M, G | GN> {
    const elements = builder({
      element: makeCustomGranularityElementInitMaker(),
    });
    return this.unsafeWithGranularity(granularityName, elements, "temporal");
  }
  getMetric(name: string & keyof M) {
    const metric = this.metrics[name];
    invariant(metric, `Metric ${name} not found in model ${this.name}`);
    return metric;
  }
  getDimension(name: string & keyof D) {
    const dimension = this.dimensions[name];
    invariant(dimension, `Dimension ${name} not found in model ${this.name}`);
    return dimension;
  }
  getPrimaryKeyDimensions() {
    return Object.values(this.dimensions).filter((d) => d.props.primaryKey);
  }
  getMember(name: string & (keyof D | keyof M)) {
    const member = this.dimensions[name] || this.metrics[name];
    invariant(member, `Member ${name} not found in model ${this.name}`);
    return member;
  }
  getDimensions() {
    return Object.values(this.dimensions);
  }
  getMetrics() {
    return Object.values(this.metrics);
  }
  getTableName(dialect: AnyBaseDialect, context: C) {
    if (this.config.type === "table") {
      if (typeof this.config.name === "string") {
        return {
          sql: this.config.name
            .split(".")
            .map((v) => dialect.asIdentifier(v))
            .join("."),
          bindings: [],
        };
      }

      const result = this.config.name({
        identifier: (name: string) => new IdentifierRef(name),
        sql: (strings: TemplateStringsArray, ...values: unknown[]) =>
          new SqlWithRefs([...strings], values),
        getContext: () => context,
      });

      return result.render(dialect, context);
    }

    throw new Error("Model is not a table");
  }
  getAs(dialect: AnyBaseDialect, context: C) {
    if (this.config.type === "sqlQuery") {
      return { sql: dialect.asIdentifier(this.config.alias), bindings: [] };
    }

    return this.getTableName(dialect, context);
  }
  getSql(dialect: AnyBaseDialect, context: C) {
    invariant(this.config.type === "sqlQuery", "Model is not an SQL query");

    const result = this.config.sql({
      identifier: (name: string) => new IdentifierRef(name),
      sql: (strings: TemplateStringsArray, ...values: unknown[]) =>
        new SqlWithRefs([...strings], values),
      getContext: () => context,
    });
    return result.render(dialect, context);
  }
  clone<N extends string>(name: N) {
    const newModel = new Model<C, N, D, M, G>(name, this.config);
    for (const [key, value] of Object.entries(this.dimensions)) {
      newModel.dimensions[key] = value.clone(newModel);
    }
    for (const [key, value] of Object.entries(this.metrics)) {
      newModel.metrics[key] = value.clone(newModel);
    }
    newModel.temporalGranularities.push(...this.temporalGranularities);
    newModel.categoricalGranularities.push(...this.categoricalGranularities);
    for (const granularityName of this.granularitiesNames) {
      newModel.granularitiesNames.add(granularityName);
    }
    return newModel;
  }
}

const VALID_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

export function model<C = undefined>() {
  return {
    withName: <N extends string>(name: N) => {
      invariant(VALID_NAME_RE.test(name), `Invalid model name: ${name}`);

      return {
        fromTable: (tableName?: string | ModelSqlFn<C>) => {
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
