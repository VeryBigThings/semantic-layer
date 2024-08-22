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
  BasicMetric,
  BasicMetricProps,
  DimensionHasTemporalGranularity,
  WithTemporalGranularityDimensions,
} from "./model/member.js";
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
  C,
  N extends string,
  D extends MemberNameToType = MemberNameToType,
  M extends MemberNameToType = MemberNameToType,
  G extends string = never,
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
    public readonly name: N,
    public readonly config: ModelConfig<C>,
  ) {}
  withDimension<
    DN1 extends string,
    DP extends BasicDimensionProps<C, string & keyof D>,
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

    this.dimensions[name] = new BasicDimension(this, name, dimension);
    if (
      // TODO: figure out why typeHasGranularity is not working anymore
      dimension.type === "datetime" ||
      dimension.type === "date" ||
      (dimension.type === "time" && dimension.omitGranularity !== true)
    ) {
      const granularityDimensions =
        TemporalGranularityByDimensionType[dimension.type];
      for (const g of granularityDimensions) {
        const { format: _format, ...dimensionWithoutFormat } = dimension;
        this.dimensions[`${name}.${g}`] = new BasicDimension(
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
      this.unsafeWithHierarchy(
        name,
        makeTemporalHierarchyElementsForDimension(name, dimension.type),
        "temporal",
      );
    }
    return this;
  }
  withMetric<
    MN1 extends string,
    MP extends BasicMetricProps<C, string & keyof D>,
  >(
    name: Exclude<MN1, keyof M | keyof D>,
    metric: MP,
  ): Model<C, N, D, M & { [k in MN1]: MP["type"] }, G> {
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
      `Granularity ${hierarchyName} already exists`,
    );
    this.hierarchyNames.add(hierarchyName);
    if (type === "categorical") {
      this.categoricalHierarchies.push({ name: hierarchyName, elements });
    } else if (type === "temporal") {
      this.temporalHierarchies.push({ name: hierarchyName, elements });
    }
    return this;
  }
  withCategoricalHierarchy<GN extends string>(
    hierarchyName: Exclude<GN, G>,
    builder: (args: {
      element: ReturnType<typeof makeHierarchyElementInitMaker<D>>;
    }) => [AnyHierarchyElement, ...AnyHierarchyElement[]],
  ): Model<C, N, D, M, G | GN> {
    const elements = builder({
      element: makeHierarchyElementInitMaker(),
    });
    return this.unsafeWithHierarchy(hierarchyName, elements, "categorical");
  }
  withTemporalHierarchy<GN extends string>(
    hierarchyName: Exclude<GN, G>,
    builder: (args: {
      element: ReturnType<typeof makeHierarchyElementInitMaker<D>>;
    }) => [AnyHierarchyElement, ...AnyHierarchyElement[]],
  ): Model<C, N, D, M, G | GN> {
    const elements = builder({
      element: makeHierarchyElementInitMaker(),
    });
    return this.unsafeWithHierarchy(hierarchyName, elements, "temporal");
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
  getTableName(repository: AnyRepository, dialect: AnyBaseDialect, context: C) {
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

    return result.render(repository, dialect);
  }
  getSql(repository: AnyRepository, dialect: AnyBaseDialect, context: C) {
    invariant(this.config.type === "sqlQuery", "Model is not an SQL query");

    const result = this.config.sql({
      identifier: (name: string) => new IdentifierRef(name),
      sql: (strings: TemplateStringsArray, ...values: unknown[]) =>
        new SqlFn([...strings], values),
      getContext: () => context,
    });
    return result.render(repository, dialect);
  }
  getTableNameOrSql(
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: C,
  ) {
    if (this.config.type === "table") {
      const { sql, bindings } = this.getTableName(repository, dialect, context);
      return dialect.fragment(sql, bindings);
    }

    const modelSql = this.getSql(repository, dialect, context);
    return dialect.fragment(
      `(${modelSql.sql}) as ${dialect.asIdentifier(this.config.alias)}`,
      modelSql.bindings,
    );
  }

  getAs(repository: AnyRepository, dialect: AnyBaseDialect, context: C) {
    if (this.config.type === "sqlQuery") {
      return SqlFragment.make({
        sql: dialect.asIdentifier(this.config.alias),
        bindings: [],
      });
    }

    return this.getTableName(repository, dialect, context);
  }

  clone<N extends string>(name: N) {
    const newModel = new Model<C, N, D, M, G>(name, this.config);
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
