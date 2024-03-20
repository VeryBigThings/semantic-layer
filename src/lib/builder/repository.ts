import { FilterType, MemberNameToType } from "../../types.js";
import {
  AnyFilterFragmentBuilderRegistry,
  GetFilterFragmentBuilderRegistryPayload,
  defaultFilterFragmentBuilderRegistry,
} from "../query/filter-builder.js";
import {
  JOIN_WEIGHTS,
  Join,
  JoinDimensionRef,
  JoinDimensions,
  JoinFn,
  JoinOnDef,
  REVERSED_JOIN,
} from "./join.js";
import { AnyModel, Model } from "./model.js";

import graphlib from "@dagrejs/graphlib";
import invariant from "tiny-invariant";
import { BaseDialect } from "../dialect/base.js";
import { QueryBuilder } from "../query/builder.js";

// biome-ignore lint/suspicious/noExplicitAny: Using any for inference
export type ModelN<T> = T extends Model<infer N, any, any> ? N : never;
// biome-ignore lint/suspicious/noExplicitAny: Using any for inference
export type ModelD<T> = T extends Model<infer N, infer D, any>
  ? { [K in string & keyof D as `${N}.${K}`]: D[K] }
  : never;
// biome-ignore lint/suspicious/noExplicitAny: Using any for inference
export type ModelM<T> = T extends Model<infer N, any, infer M>
  ? { [K in string & keyof M as `${N}.${K}`]: M[K] }
  : never;

// biome-ignore lint/suspicious/noExplicitAny: Using any for inference
export type AnyRepository = Repository<any, any, any, any>;

export class Repository<
  N extends string = never,
  D extends MemberNameToType = MemberNameToType,
  M extends MemberNameToType = MemberNameToType,
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
    return this as Repository<N | ModelN<T>, D & ModelD<T>, M & ModelM<T>, F>;
  }

  public withFilterFragmentBuilderRegistry<
    T extends AnyFilterFragmentBuilderRegistry,
  >(filterFragmentBuilderRegistry: T) {
    this.filterFragmentBuilderRegistry = filterFragmentBuilderRegistry;
    return this as Repository<
      N,
      D,
      M,
      GetFilterFragmentBuilderRegistryPayload<T>
    >;
  }

  getFilterBuilder(
    repository: Repository,
    dialect: BaseDialect,
    filterType: FilterType,
    referencedModels: string[],
    metricPrefixes?: Record<string, string>,
  ) {
    return this.filterFragmentBuilderRegistry.getFilterBuilder(
      repository,
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
    joinSqlDefFn: JoinFn<string & keyof D, N1, N2>,
  ) {
    const model1 = this.models[modelName1];
    const model2 = this.models[modelName2];
    invariant(model1, `Model ${model1} not found in repository`);
    invariant(model2, `Model ${model2} not found in repository`);
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
    } as JoinDimensions<string & keyof D, N1, N2>;

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
    joinSqlDefFn: JoinFn<string & keyof D, N1, N2>,
  ) {
    return this.join("oneToOne", model1, model2, joinSqlDefFn);
  }

  joinOneToMany<N1 extends string, N2 extends string>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<string & keyof D, N1, N2>,
  ) {
    return this.join("oneToMany", model1, model2, joinSqlDefFn);
  }

  joinManyToOne<N1 extends string, N2 extends string>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<string & keyof D, N1, N2>,
  ) {
    return this.join("manyToOne", model1, model2, joinSqlDefFn);
  }

  joinManyToMany<N1 extends string, N2 extends string>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<string & keyof D, N1, N2>,
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

  build() {
    return new QueryBuilder<D, M, F>(this, BaseDialect);
  }
}

export function repository() {
  return new Repository();
}
