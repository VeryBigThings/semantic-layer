import {
  AnyHierarchyElement,
  makeHierarchyElementInitMaker,
} from "./hierarchy.js";
import {
  HierarchyType,
  MemberNameToType,
  MemberType,
  TemporalGranularityByDimensionType,
  TemporalGranularityIndex,
  makeTemporalHierarchyElementsForDimension,
} from "./types.js";

import invariant from "tiny-invariant";
import { AnyBaseDialect } from "./dialect/base.js";

import {
  BasicDimension,
  BasicDimensionProps,
  DimensionHasTemporalGranularity,
  WithTemporalGranularityDimensions,
} from "./model/basic-dimension.js";
import { BasicMetric, BasicMetricProps } from "./model/basic-metric.js";
import { GranularityDimension } from "./model/granularity-dimension.js";
import { QueryContext } from "./query-builder/query-plan/query-context.js";
import { AnyRepository } from "./repository.js";
import { SqlFragment } from "./sql-builder.js";
import { IdentifierRef, SqlFn } from "./sql-fn.js";

export type AnyModel<C = any> = Model<
  C,
  string,
  { [k in string]: MemberType },
  { [k in string]: MemberType },
  any
>;
export type ModelConfig<C> =
  | { type: "table"; name: string | ModelSqlFn<C> }
  | { type: "sqlQuery"; alias: string; sql: ModelSqlFn<C> };

export type ModelSqlFn<C> = (args: {
  identifier: (name: string) => IdentifierRef;
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlFn;
  getContext: () => C;
}) => SqlFn;

/*function typeHasGranularity(
  type: string,
): type is keyof TemporalGranularityByDimensionType {
  return type in TemporalGranularityByDimensionType;
}*/

export class Model<
  TContext,
  TModelName extends string,
  TModelDimensions extends MemberNameToType = MemberNameToType,
  TModelMetrics extends MemberNameToType = MemberNameToType,
  TPrivateMembers extends string = never,
  TModelHierarchyNames extends string = never,
> {
  public readonly dimensions: Record<string, BasicDimension> = {};
  public readonly metrics: Record<string, BasicMetric> = {};
  public readonly categoricalHierarchies: {
    name: string;
    elements: AnyHierarchyElement[];
  }[] = [];
  public readonly temporalHierarchies: {
    name: string;
    elements: AnyHierarchyElement[];
  }[] = [];
  public readonly hierarchyNames: Set<string> = new Set();

  constructor(
    public readonly name: TModelName,
    public readonly config: ModelConfig<TContext>,
  ) {}
  withDimension<
    TDimensionName extends string,
    TDimensionProps extends BasicDimensionProps<
      TContext,
      string & keyof TModelDimensions
    >,
    TDimensionIsPrivate extends
      boolean = TDimensionProps["private"] extends true ? true : false,
    TDimensionHasTemporalGranularity extends
      boolean = DimensionHasTemporalGranularity<TDimensionProps>,
  >(
    name: Exclude<TDimensionName, keyof TModelDimensions | keyof TModelMetrics>,
    dimensionProps: TDimensionProps,
  ): Model<
    TContext,
    TModelName,
    TDimensionHasTemporalGranularity extends true
      ? TModelDimensions &
          WithTemporalGranularityDimensions<
            TDimensionName,
            TDimensionProps["type"]
          >
      : TModelDimensions & { [k in TDimensionName]: TDimensionProps["type"] },
    TModelMetrics,
    TDimensionIsPrivate extends true
      ?
          | TPrivateMembers
          | (TDimensionIsPrivate extends true
              ? string &
                  keyof WithTemporalGranularityDimensions<
                    TDimensionName,
                    TDimensionProps["type"]
                  >
              : TDimensionName)
      : TPrivateMembers,
    TDimensionHasTemporalGranularity extends true
      ? TDimensionIsPrivate extends true
        ? TModelHierarchyNames
        : TModelHierarchyNames | TDimensionName
      : TModelHierarchyNames
  > {
    invariant(
      !(this.dimensions[name] || this.metrics[name]),
      `Member "${name}" already exists`,
    );
    const dimension = new BasicDimension(this, name, dimensionProps);

    this.dimensions[name] = dimension;
    if (
      // TODO: figure out why typeHasGranularity is not working anymore
      (dimensionProps.type === "datetime" ||
        dimensionProps.type === "date" ||
        dimensionProps.type === "time") &&
      dimensionProps.omitGranularity !== true
    ) {
      const granularityDimensions =
        TemporalGranularityByDimensionType[dimensionProps.type];
      for (const g of granularityDimensions) {
        const { format: _format, ...dimensionWithoutFormat } = dimensionProps;
        this.dimensions[`${name}.${g}`] = new GranularityDimension(
          this,
          dimension,
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
      if (!dimensionProps.private) {
        this.unsafeWithHierarchy(
          name,
          makeTemporalHierarchyElementsForDimension(name, dimensionProps.type),
          "temporal",
        );
      }
    }
    return this;
  }
  withMetric<
    TMetricName extends string,
    TMetricProps extends BasicMetricProps<
      TContext,
      string & keyof TModelDimensions,
      string & keyof TModelMetrics
    >,
    TMetricIsPrivate extends boolean = TMetricProps["private"] extends true
      ? true
      : false,
  >(
    name: Exclude<TMetricName, keyof TModelMetrics | keyof TModelDimensions>,
    metric: TMetricProps,
  ): Model<
    TContext,
    TModelName,
    TModelDimensions,
    TModelMetrics & { [k in TMetricName]: TMetricProps["type"] },
    TMetricIsPrivate extends true
      ? TPrivateMembers | TMetricName
      : TPrivateMembers,
    TModelHierarchyNames
  > {
    invariant(
      !(this.dimensions[name] || this.metrics[name]),
      `Member "${name}" already exists`,
    );

    this.metrics[name] = new BasicMetric(this, name, metric);
    return this;
  }
  unsafeWithHierarchy(
    hierarchyName: string,
    elements: AnyHierarchyElement[],
    type: HierarchyType,
  ) {
    invariant(
      this.hierarchyNames.has(hierarchyName) === false,
      `Hierarchy ${hierarchyName} already exists`,
    );
    this.hierarchyNames.add(hierarchyName);
    if (type === "categorical") {
      this.categoricalHierarchies.push({ name: hierarchyName, elements });
    } else if (type === "temporal") {
      this.temporalHierarchies.push({ name: hierarchyName, elements });
    }
    return this;
  }
  withCategoricalHierarchy<THierarchyName extends string>(
    hierarchyName: Exclude<THierarchyName, TModelHierarchyNames>,
    builder: (args: {
      element: ReturnType<
        typeof makeHierarchyElementInitMaker<
          Omit<TModelDimensions, TPrivateMembers>
        >
      >;
    }) => [AnyHierarchyElement, ...AnyHierarchyElement[]],
  ): Model<
    TContext,
    TModelName,
    TModelDimensions,
    TModelMetrics,
    TPrivateMembers,
    TModelHierarchyNames | THierarchyName
  > {
    const elements = builder({
      element: makeHierarchyElementInitMaker(),
    });
    return this.unsafeWithHierarchy(hierarchyName, elements, "categorical");
  }
  withTemporalHierarchy<THierarchyName extends string>(
    hierarchyName: Exclude<THierarchyName, TModelHierarchyNames>,
    builder: (args: {
      element: ReturnType<
        typeof makeHierarchyElementInitMaker<
          Omit<TModelDimensions, TPrivateMembers>
        >
      >;
    }) => [AnyHierarchyElement, ...AnyHierarchyElement[]],
  ): Model<
    TContext,
    TModelName,
    TModelDimensions,
    TModelMetrics,
    TPrivateMembers,
    TModelHierarchyNames | THierarchyName
  > {
    const elements = builder({
      element: makeHierarchyElementInitMaker(),
    });
    return this.unsafeWithHierarchy(hierarchyName, elements, "temporal");
  }
  getMetric(name: string & keyof TModelMetrics) {
    const metric = this.metrics[name];
    invariant(metric, `Metric ${name} not found in model ${this.name}`);
    return metric;
  }
  getDimension(name: string & keyof TModelDimensions) {
    const dimension = this.dimensions[name];
    invariant(dimension, `Dimension ${name} not found in model ${this.name}`);
    return dimension;
  }
  getPrimaryKeyDimensions() {
    return Object.values(this.dimensions).filter((d) => d.props.primaryKey);
  }
  getMember(name: string & (keyof TModelDimensions | keyof TModelMetrics)) {
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
  getTableName(
    repository: AnyRepository,
    queryContext: QueryContext,
    dialect: AnyBaseDialect,
    context: TContext,
  ) {
    invariant(this.config.type === "table", "Model is not a table");

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
        new SqlFn([...strings], values),
      getContext: () => context,
    });

    return result.render(repository, queryContext, dialect);
  }
  getSql(
    repository: AnyRepository,
    queryContext: QueryContext,
    dialect: AnyBaseDialect,
    context: TContext,
  ) {
    invariant(this.config.type === "sqlQuery", "Model is not an SQL query");

    const result = this.config.sql({
      identifier: (name: string) => new IdentifierRef(name),
      sql: (strings: TemplateStringsArray, ...values: unknown[]) =>
        new SqlFn([...strings], values),
      getContext: () => context,
    });
    return result.render(repository, queryContext, dialect);
  }
  getTableNameOrSql(
    repository: AnyRepository,
    queryContext: QueryContext,
    dialect: AnyBaseDialect,
    context: TContext,
  ) {
    if (this.config.type === "table") {
      const { sql, bindings } = this.getTableName(
        repository,
        queryContext,
        dialect,
        context,
      );
      return dialect.fragment(sql, bindings);
    }

    const modelSql = this.getSql(repository, queryContext, dialect, context);
    return dialect.fragment(
      `(${modelSql.sql}) as ${dialect.asIdentifier(this.config.alias)}`,
      modelSql.bindings,
    );
  }

  getAs(
    repository: AnyRepository,
    queryContext: QueryContext,
    dialect: AnyBaseDialect,
    context: TContext,
  ) {
    if (this.config.type === "sqlQuery") {
      return SqlFragment.make({
        sql: dialect.asIdentifier(this.config.alias),
        bindings: [],
      });
    }

    return this.getTableName(repository, queryContext, dialect, context);
  }

  clone<TNewModelName extends string>(name: TNewModelName) {
    const newModel = new Model<
      TContext,
      TNewModelName,
      TModelDimensions,
      TModelMetrics,
      TPrivateMembers,
      TModelHierarchyNames
    >(name, this.config);
    for (const [key, value] of Object.entries(this.dimensions)) {
      newModel.dimensions[key] = value.clone(newModel);
    }
    for (const [key, value] of Object.entries(this.metrics)) {
      newModel.metrics[key] = value.clone(newModel);
    }
    newModel.temporalHierarchies.push(...this.temporalHierarchies);
    newModel.categoricalHierarchies.push(...this.categoricalHierarchies);
    for (const hierarchyName of this.hierarchyNames) {
      newModel.hierarchyNames.add(hierarchyName);
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

export type GetModelContext<T> = T extends Model<
  infer TModelContext,
  any,
  any,
  any,
  any
>
  ? TModelContext
  : never;

export type GetModelName<T> = T extends Model<
  any,
  infer TModelName,
  any,
  any,
  any
>
  ? TModelName
  : never;

export type GetModelDimensions<T> = T extends Model<
  any,
  infer TModelName,
  infer TModelDimensions,
  any,
  any
>
  ? {
      [K in string &
        keyof TModelDimensions as `${TModelName}.${K}`]: TModelDimensions[K];
    }
  : never;

export type GetModelMetrics<T> = T extends Model<
  any,
  infer TModelName,
  any,
  infer TModelMetrics,
  any
>
  ? {
      [K in string &
        keyof TModelMetrics as `${TModelName}.${K}`]: TModelMetrics[K];
    }
  : never;

export type GetModelPrivateMembers<T> = T extends Model<
  any,
  infer TModelName,
  any,
  any,
  infer TPrivateMembers,
  any
>
  ? `${TModelName}.${TPrivateMembers}`
  : never;

export type GetModelHierarchies<T> = T extends Model<
  any,
  any,
  any,
  any,
  any,
  infer THierarchies
>
  ? THierarchies
  : never;
