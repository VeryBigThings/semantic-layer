import * as queryBuilder from "../query/builder.js";

import { FilterType, Query } from "../../types.js";
import {
  AnyFilterFragmentBuilderRegistry,
  GetFilterFragmentBuilderRegistryPayload,
  defaultFilterFragmentBuilderRegistry,
} from "../query/filter-builder.js";
import { AnyModel, Model } from "./model.js";

import graphlib from "@dagrejs/graphlib";
import invariant from "tiny-invariant";
import { BaseDialect } from "../dialect/base.js";

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type ModelN<T> = T extends Model<infer N, any, any> ? N : never;
// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type ModelDN<T> = T extends Model<infer N, infer DN, any>
  ? `${N}.${DN}`
  : never;
// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type ModelMN<T> = T extends Model<infer N, any, infer MN>
  ? `${N}.${MN}`
  : never;

export class JoinDimensionRef<N extends string, DN extends string> {
  constructor(
    private readonly model: N,
    private readonly dimension: DN,
  ) {}
  render(database: Database, dialect: BaseDialect) {
    return database
      .getModel(this.model)
      .getDimension(this.dimension)
      .getSql(dialect);
  }
}
export class JoinOnDef {
  constructor(
    private readonly strings: string[],
    private readonly values: unknown[],
  ) {}
  render(database: Database, dialect: BaseDialect) {
    const sql: string[] = [];
    const bindings: unknown[] = [];
    for (let i = 0; i < this.strings.length; i++) {
      sql.push(this.strings[i]!);
      if (this.values[i]) {
        const value = this.values[i];
        if (value instanceof JoinDimensionRef) {
          const result = value.render(database, dialect);
          sql.push(result.sql);
          bindings.push(...result.bindings);
        } else {
          sql.push("?");
          bindings.push(value);
        }
      }
    }
    return {
      sql: sql.join(""),
      bindings,
    };
  }
}

export interface Join {
  left: string;
  right: string;
  joinOnDef: JoinOnDef;
  reversed: boolean;
  type: "oneToOne" | "oneToMany" | "manyToOne" | "manyToMany";
}

export type JoinFn<
  DN extends string,
  N1 extends string,
  N2 extends string,
> = (args: {
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => JoinOnDef;
  dimensions: JoinDimensions<DN, N1, N2>;
}) => JoinOnDef;

export type ModelDimensionsWithoutModelPrefix<
  N extends string,
  DN extends string,
> = DN extends `${N}.${infer D}` ? D : never;

export type JoinDimensions<
  DN extends string,
  N1 extends string,
  N2 extends string,
> = {
  [TK in N1]: {
    [DK in ModelDimensionsWithoutModelPrefix<N1, DN>]: JoinDimensionRef<TK, DK>;
  };
} & {
  [TK in N2]: {
    [DK in ModelDimensionsWithoutModelPrefix<N2, DN>]: JoinDimensionRef<TK, DK>;
  };
};

const JOIN_WEIGHTS: Record<Join["type"], number> = {
  oneToOne: 1,
  oneToMany: 3,
  manyToOne: 2,
  manyToMany: 4,
};

const REVERSED_JOIN: Record<Join["type"], Join["type"]> = {
  oneToOne: "oneToOne",
  oneToMany: "manyToOne",
  manyToOne: "oneToMany",
  manyToMany: "manyToMany",
};

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type AnyDatabase = Database<any, any, any, any>;

export class Database<
  N extends string = never,
  DN extends string = never,
  MN extends string = never,
  F = GetFilterFragmentBuilderRegistryPayload<
    ReturnType<typeof defaultFilterFragmentBuilderRegistry>
  >,
> {
  private readonly models: Record<string, AnyModel> = {};
  private filterFragmentBuilderRegistry: AnyFilterFragmentBuilderRegistry =
    defaultFilterFragmentBuilderRegistry();
  readonly joins: Record<string, Record<string, Join>> = {};
  readonly graph: graphlib.Graph = new graphlib.Graph();
  readonly dimensionsIndex: Record<
    string,
    { model: string; dimension: string }
  > = {} as Record<string, { model: string; dimension: string }>;
  readonly metricsIndex: Record<string, { model: string; metric: string }> =
    {} as Record<string, { model: string; metric: string }>;

  public withModel<T extends AnyModel>(model: T) {
    this.models[model.name] = model;
    for (const dimension in model.dimensions) {
      this.dimensionsIndex[`${model.name}.${dimension}`] = {
        model: model.name,
        dimension,
      };
    }
    for (const metric in model.metrics) {
      this.metricsIndex[`${model.name}.${metric}`] = {
        model: model.name,
        metric,
      };
    }
    return this as Database<N | ModelN<T>, DN | ModelDN<T>, MN | ModelMN<T>, F>;
  }

  public withFilterFragmentBuilderRegistry<
    T extends AnyFilterFragmentBuilderRegistry,
  >(filterFragmentBuilderRegistry: T) {
    this.filterFragmentBuilderRegistry = filterFragmentBuilderRegistry;
    return this as Database<
      N,
      DN,
      MN,
      GetFilterFragmentBuilderRegistryPayload<T>
    >;
  }

  getFilterBuilder(
    database: Database,
    dialect: BaseDialect,
    filterType: FilterType,
    referencedModels: string[],
    metricPrefixes?: Record<string, string>,
  ) {
    return this.filterFragmentBuilderRegistry.getFilterBuilder(
      database,
      dialect,
      filterType,
      referencedModels,
      metricPrefixes,
    );
  }

  join<N1 extends string, N2 extends string>(
    type: Join["type"],
    modelName1: N1,
    modelName2: N2,
    joinSqlDefFn: JoinFn<DN, N1, N2>,
  ) {
    const model1 = this.models[modelName1];
    const model2 = this.models[modelName2];
    invariant(model1, `Model ${model1} not found in database`);
    invariant(model2, `Model ${model2} not found in database`);
    const dimensions = {
      [model1.name]: Object.keys(model1.dimensions).reduce<
        Record<string, JoinDimensionRef<string, string>>
      >((acc, dimension) => {
        acc[dimension] = new JoinDimensionRef(modelName1, dimension);
        return acc;
      }, {}),
      [model2.name]: Object.keys(model2.dimensions).reduce<
        Record<string, JoinDimensionRef<string, string>>
      >((acc, dimension) => {
        acc[dimension] = new JoinDimensionRef(model2.name, dimension);
        return acc;
      }, {}),
    } as JoinDimensions<DN, N1, N2>;

    const joinSqlDef = joinSqlDefFn({
      sql: (strings, ...values) => new JoinOnDef([...strings], values),
      dimensions,
    });

    const reversedType = REVERSED_JOIN[type];

    this.graph.setEdge(model1.name, model2.name, JOIN_WEIGHTS[type]);
    this.graph.setEdge(model2.name, model1.name, JOIN_WEIGHTS[reversedType]);

    this.joins[model1.name] ||= {};
    this.joins[model1.name]![model2.name] = {
      left: model1.name,
      right: model2.name,
      joinOnDef: joinSqlDef,
      type: type,
      reversed: false,
    };
    this.joins[model2.name] ||= {};
    this.joins[model2.name]![model1.name] = {
      left: model2.name,
      right: model1.name,
      joinOnDef: joinSqlDef,
      type: reversedType,
      reversed: true,
    };
    return this;
  }

  joinOneToOne<N1 extends string, N2 extends string>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<DN, N1, N2>,
  ) {
    return this.join("oneToOne", model1, model2, joinSqlDefFn);
  }

  joinOneToMany<N1 extends string, N2 extends string>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<DN, N1, N2>,
  ) {
    return this.join("oneToMany", model1, model2, joinSqlDefFn);
  }

  joinManyToOne<N1 extends string, N2 extends string>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<DN, N1, N2>,
  ) {
    return this.join("manyToOne", model1, model2, joinSqlDefFn);
  }

  joinManyToMany<N1 extends string, N2 extends string>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<DN, N1, N2>,
  ) {
    return this.join("manyToMany", model1, model2, joinSqlDefFn);
  }

  getDimension(dimensionName: string) {
    invariant(
      this.dimensionsIndex[dimensionName],
      `Dimension ${dimensionName} not found`,
    );
    const { model: modelName, dimension } =
      this.dimensionsIndex[dimensionName]!;
    const model = this.models[modelName];
    if (!model) {
      throw new Error(`Model ${modelName} not found`);
    }
    return model.getDimension(dimension);
  }

  getMetric(metricName: string) {
    invariant(this.metricsIndex[metricName], `Metric ${metricName} not found`);
    const { model: modelName, metric } = this.metricsIndex[metricName]!;
    const model = this.models[modelName];
    if (!model) {
      throw new Error(`Model ${modelName} not found`);
    }
    return model.getMetric(metric);
  }

  getMember(memberName: string) {
    if (this.dimensionsIndex[memberName]) {
      return this.getDimension(memberName);
    }
    if (this.metricsIndex[memberName]) {
      return this.getMetric(memberName);
    }
    throw new Error(`Member ${memberName} not found`);
  }

  getModel(modelName: string) {
    const model = this.models[modelName];
    if (!model) {
      throw new Error(`Model ${modelName} not found`);
    }
    return model;
  }

  getModelJoins(modelName: string) {
    return Object.values(this.joins[modelName] ?? {});
  }

  getJoin(modelName: string, joinModelName: string) {
    return this.joins[modelName]?.[joinModelName];
  }

  query(query: Query<DN, MN, F & { member: DN | MN }>) {
    const graphComponents = graphlib.alg.components(this.graph);
    if (graphComponents.length > 1) {
      throw new Error("Database graph must be a single connected component");
    }

    const DialectClass = BaseDialect;

    return queryBuilder.build(this, DialectClass, query);
  }
}

export function database() {
  return new Database();
}
