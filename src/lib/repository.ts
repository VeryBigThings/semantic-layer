import {
  AvailableDialects,
  AvailableDialectsNames,
  DialectParamsReturnType,
} from "./dialect.js";
import {
  AnyHierarchyElement,
  makeHierarchyElementInitMaker,
} from "./hierarchy.js";
import {
  AnyJoin,
  JOIN_WEIGHTS,
  JoinDimensions,
  JoinFn,
  REVERSED_JOIN,
  makeModelJoinPayload,
} from "./join.js";
import { AnyModel, Model } from "./model.js";

import {
  AnyFilterFragmentBuilderRegistry,
  GetFilterFragmentBuilderRegistryPayload,
  defaultFilterFragmentBuilderRegistry,
} from "./query-builder/filter-builder.js";
import { HierarchyType, MemberNameToType } from "./types.js";

import graphlib from "@dagrejs/graphlib";
import invariant from "tiny-invariant";
import { Dimension, Metric } from "./member.js";
import { QueryBuilder } from "./query-builder.js";
import { IdentifierRef, SqlFn } from "./sql-fn.js";

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
  public readonly categoricalHierarchies: {
    name: string;
    elements: AnyHierarchyElement[];
  }[] = [];
  public readonly temporalHierarchies: {
    name: string;
    elements: AnyHierarchyElement[];
  }[] = [];
  public readonly hierarchyNames: Set<string> = new Set();

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

    return this as unknown as Repository<
      C,
      N | ModelN<T>,
      D & ModelD<T>,
      M & ModelM<T>,
      F,
      G | `${ModelN<T>}.${ModelG<T>}`
    >;
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
  ): Repository<C, N, D, M, F, G | GN> {
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
  ): Repository<C, N, D, M, F, G | GN> {
    const elements = builder({
      element: makeHierarchyElementInitMaker(),
    });
    return this.unsafeWithHierarchy(hierarchyName, elements, "temporal");
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
      const models = [model1, model2].reduce(
        (acc, model) => {
          acc[model.name as N1 | N2] = makeModelJoinPayload(model, context);
          return acc;
        },
        {} as JoinDimensions<string & keyof D, N1 | N2>,
      );

      return joinSqlDefFn({
        sql: (strings, ...values) => new SqlFn([...strings], values),
        identifier: (name) => new IdentifierRef(name),
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
    const dimensionIndexEntry = this.dimensionsIndex[dimensionName];
    invariant(dimensionIndexEntry, `Dimension ${dimensionName} not found`);
    const { model: modelName, dimension } = dimensionIndexEntry;
    const model = this.models[modelName];
    invariant(model, `Model ${modelName} not found`);
    return model.getDimension(dimension);
  }

  getMetric(metricName: string): Metric {
    const metricIndexEntry = this.metricsIndex[metricName];
    invariant(metricIndexEntry, `Metric ${metricName} not found`);
    const { model: modelName, metric } = metricIndexEntry;
    const model = this.models[modelName];
    invariant(model, `Model ${modelName} not found`);
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
  getModels() {
    return Object.values(this.models);
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
    return new QueryBuilder<C, D, M, F, P, G>(this, dialect);
  }
}

export function repository<C = undefined>() {
  return new Repository<C>();
}
