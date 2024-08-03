import {
  AvailableDialects,
  AvailableDialectsNames,
  DialectParamsReturnType,
} from "./dialect.js";
import {
  AnyJoin,
  JOIN_WEIGHTS,
  JoinDimensions,
  JoinFn,
  JoinIdentifierRef,
  JoinOnDef,
  REVERSED_JOIN,
  makeModelJoinPayload,
} from "./join.js";
import { AnyModel, Model } from "./model.js";
import type { Dimension, Metric } from "./model.js";
import {
  AnyFilterFragmentBuilderRegistry,
  GetFilterFragmentBuilderRegistryPayload,
  defaultFilterFragmentBuilderRegistry,
} from "./query-builder/filter-builder.js";
import {
  CustomGranularity,
  CustomGranularityElements,
  GranularityType,
  MemberNameToType,
} from "./types.js";

import graphlib from "@dagrejs/graphlib";
import invariant from "tiny-invariant";
import { QueryBuilder } from "./query-builder.js";

export type ModelC<T> = T extends Model<infer C, any, any, any, any>
  ? C
  : never;

export type ModelN<T> = T extends Model<any, infer N, any, any, any>
  ? N
  : never;

export type ModelD<T> = T extends Model<any, infer N, infer D, any, any>
  ? { [K in string & keyof D as `${N}.${K}`]: D[K] }
  : never;

export type ModelM<T> = T extends Model<any, infer N, any, infer M, any>
  ? { [K in string & keyof M as `${N}.${K}`]: M[K] }
  : never;

export type ModelG<T> = T extends Model<any, any, any, any, infer G>
  ? G
  : never;

export type ModelWithMatchingContext<C, T extends AnyModel> = [C] extends [
  ModelC<T>,
]
  ? T
  : never;

export type AnyRepository = Repository<any, any, any, any, any, any>;

export class Repository<
  C,
  N extends string = never,
  D extends MemberNameToType = MemberNameToType,
  M extends MemberNameToType = MemberNameToType,
  F = GetFilterFragmentBuilderRegistryPayload<
    ReturnType<typeof defaultFilterFragmentBuilderRegistry>
  >,
  G extends string = never,
> {
  private readonly models: Record<string, AnyModel> = {};
  private filterFragmentBuilderRegistry: AnyFilterFragmentBuilderRegistry =
    defaultFilterFragmentBuilderRegistry();
  readonly joins: Record<string, Record<string, AnyJoin>> = {};
  readonly graph: graphlib.Graph = new graphlib.Graph();
  readonly dimensionsIndex: Record<
    string,
    { model: string; dimension: string }
  > = {} as Record<string, { model: string; dimension: string }>;
  readonly metricsIndex: Record<string, { model: string; metric: string }> =
    {} as Record<string, { model: string; metric: string }>;
  readonly granularities: CustomGranularity[] = [];
  readonly granularitiesNames: Set<string> = new Set();

  withModel<T extends AnyModel>(model: ModelWithMatchingContext<C, T>) {
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
    for (const granularity of Object.values(model.granularities)) {
      this.unsafeWithGranularity(
        `${model.name}.${granularity.name}`,
        granularity.elements.map((element) => {
          if (typeof element === "string") {
            return `${model.name}.${element}`;
          }
          const { key, elements, display } = element;
          const namespacedDisplay =
            display === undefined
              ? undefined
              : typeof display === "string"
                ? `${model.name}.${display}`
                : display.map((element) => `${model.name}.${element}`);
          const namespacedElements = elements.map(
            (element) => `${model.name}.${element}`,
          );
          return {
            key,
            elements: namespacedElements,
            display: namespacedDisplay,
          };
        }),
        granularity.type ?? "custom",
      );
    }

    return this as unknown as Repository<
      C,
      N | ModelN<T>,
      D & ModelD<T>,
      M & ModelM<T>,
      F,
      G | `${ModelN<T>}.${ModelG<T>}`
    >;
  }

  unsafeWithGranularity(
    granularityName: string,
    elements: CustomGranularityElements,
    type: GranularityType,
    position: "top" | "bottom" = "bottom",
  ) {
    invariant(
      this.granularitiesNames.has(granularityName) === false,
      `Granularity ${granularityName} already exists`,
    );
    this.granularitiesNames.add(granularityName);
    if (position === "top") {
      this.granularities.unshift({
        name: granularityName,
        type,
        elements,
      });
    } else {
      this.granularities.push({
        name: granularityName,
        type,
        elements,
      });
    }
    return this;
  }

  withGranularity<GN extends string>(
    granularityName: Exclude<GN, G>,
    elements: CustomGranularityElements<Extract<keyof M | keyof D, string>>,
    type: GranularityType = "custom",
  ): Repository<C, N, D, M, F, G | GN> {
    return this.unsafeWithGranularity(granularityName, elements, type, "top");
  }

  withFilterFragmentBuilderRegistry<T extends AnyFilterFragmentBuilderRegistry>(
    filterFragmentBuilderRegistry: T,
  ) {
    this.filterFragmentBuilderRegistry = filterFragmentBuilderRegistry;
    return this as Repository<
      C,
      N,
      D,
      M,
      GetFilterFragmentBuilderRegistryPayload<T>,
      G
    >;
  }

  getFilterFragmentBuilderRegistry() {
    return this.filterFragmentBuilderRegistry;
  }

  join<N1 extends N, N2 extends N & Exclude<N, N1>>(
    type: AnyJoin["type"],
    modelName1: N1,
    modelName2: N2,
    joinSqlDefFn: JoinFn<C, string & keyof D, N1, N2>,
  ) {
    const model1 = this.models[modelName1];
    const model2 = this.models[modelName2];

    invariant(model1, `Model ${model1} not found in repository`);
    invariant(model2, `Model ${model2} not found in repository`);
    invariant(
      model1.name !== model2.name,
      `Model ${model1.name} cannot be joined to itself`,
    );

    const joinSqlDef = (context: C) => {
      const models = {
        [model1.name]: makeModelJoinPayload(model1, context),
        [model2.name]: makeModelJoinPayload(model2, context),
      } as JoinDimensions<string & keyof D, N1, N2>;

      return joinSqlDefFn({
        sql: (strings, ...values) => new JoinOnDef([...strings], values),
        identifier: (name) => new JoinIdentifierRef(name),
        models,
        getContext: () => context,
      });
    };

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

  joinOneToOne<N1 extends N, N2 extends N & Exclude<N, N1>>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<C, string & keyof D, N1, N2>,
  ) {
    return this.join("oneToOne", model1, model2, joinSqlDefFn);
  }

  joinOneToMany<N1 extends N, N2 extends N & Exclude<N, N1>>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<C, string & keyof D, N1, N2>,
  ) {
    return this.join("oneToMany", model1, model2, joinSqlDefFn);
  }

  joinManyToOne<N1 extends N, N2 extends N & Exclude<N, N1>>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<C, string & keyof D, N1, N2>,
  ) {
    return this.join("manyToOne", model1, model2, joinSqlDefFn);
  }

  joinManyToMany<N1 extends N, N2 extends N & Exclude<N, N1>>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<C, string & keyof D, N1, N2>,
  ) {
    return this.join("manyToMany", model1, model2, joinSqlDefFn);
  }

  getDimension(dimensionName: string): Dimension {
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

  getMetric(metricName: string): Metric {
    invariant(this.metricsIndex[metricName], `Metric ${metricName} not found`);
    const { model: modelName, metric } = this.metricsIndex[metricName]!;
    const model = this.models[modelName];
    if (!model) {
      throw new Error(`Model ${modelName} not found`);
    }
    return model.getMetric(metric);
  }

  getMember(memberName: string): Metric | Dimension {
    if (this.dimensionsIndex[memberName]) {
      return this.getDimension(memberName);
    }
    if (this.metricsIndex[memberName]) {
      return this.getMetric(memberName);
    }
    throw new Error(`Member ${memberName} not found`);
  }

  getDimensions(): Dimension[] {
    return Object.values(this.models).flatMap((m) => m.getDimensions());
  }

  getMetrics(): Metric[] {
    return Object.values(this.models).flatMap((m) => m.getMetrics());
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

  getJoins() {
    return Object.values(this.joins)
      .flatMap((joins) => Object.values(joins))
      .filter((join) => !join.reversed);
  }

  build<N extends AvailableDialectsNames, P = DialectParamsReturnType<N>>(
    dialectName: N,
  ) {
    const dialect = AvailableDialects[dialectName];
    return new QueryBuilder<C, D, M, F, P>(this, dialect);
  }
}

export function repository<C = undefined>() {
  return new Repository<C>();
}
