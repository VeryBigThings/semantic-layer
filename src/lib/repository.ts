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
  JoinFn,
  JoinFnModels,
  JoinOptions,
  REVERSED_JOIN,
  makeModelJoinPayload,
} from "./join.js";
import {
  AnyModel,
  GetModelContext,
  GetModelDimensions,
  GetModelHierarchies,
  GetModelMetrics,
  GetModelName,
  GetModelPrivateMembers,
} from "./model.js";

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
import {
  CalculatedDimension,
  CalculatedDimensionProps,
} from "./repository/calculated-dimension.js";
import {
  CalculatedMetric,
  CalculatedMetricProps,
} from "./repository/calculated-metric.js";
import { IdentifierRef, SqlFn } from "./sql-fn.js";

export type ModelWithMatchingContext<C, T extends AnyModel> = [C] extends [
  GetModelContext<T>,
]
  ? T
  : never;

export type AnyRepository = Repository<any, any, any, any, any, any>;

export class Repository<
  TContext,
  TModelNames extends string = never,
  TDimensions extends MemberNameToType = MemberNameToType,
  TMetrics extends MemberNameToType = MemberNameToType,
  TPrivateMembers extends string = never,
  TFilters = GetFilterFragmentBuilderRegistryPayload<
    ReturnType<typeof defaultFilterFragmentBuilderRegistry>
  >,
  THierarchies extends string = never,
> {
  private readonly models: Record<string, AnyModel> = {};
  private filterFragmentBuilderRegistry: AnyFilterFragmentBuilderRegistry =
    defaultFilterFragmentBuilderRegistry();
  readonly joins: Record<string, Record<string, AnyJoin>> = {};
  readonly graph: graphlib.Graph = new graphlib.Graph();
  readonly calculatedDimensions: Record<string, CalculatedDimension> = {};
  readonly calculatedMetrics: Record<string, CalculatedMetric> = {};
  readonly dimensionsIndex: Record<
    string,
    { model?: string; dimension: string }
  > = {} as Record<string, { model?: string; dimension: string }>;
  readonly metricsIndex: Record<string, { model?: string; metric: string }> =
    {} as Record<string, { model?: string; metric: string }>;
  public readonly categoricalHierarchies: {
    name: string;
    elements: AnyHierarchyElement[];
  }[] = [];
  public readonly temporalHierarchies: {
    name: string;
    elements: AnyHierarchyElement[];
  }[] = [];
  public readonly hierarchyNames: Set<string> = new Set();

  withModel<T extends AnyModel>(model: ModelWithMatchingContext<TContext, T>) {
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
      TContext,
      TModelNames | GetModelName<T>,
      TDimensions & GetModelDimensions<T>,
      TMetrics & GetModelMetrics<T>,
      TPrivateMembers | GetModelPrivateMembers<T>,
      TFilters,
      THierarchies | `${GetModelName<T>}.${GetModelHierarchies<T>}`
    >;
  }

  withCalculatedDimension<
    TCalculatedDimensionName extends string,
    TCalculatedDimensionProps extends CalculatedDimensionProps<
      TContext,
      TModelNames,
      keyof TDimensions & string
    >,
  >(
    path: Exclude<TCalculatedDimensionName, keyof TDimensions | keyof TMetrics>,
    props: TCalculatedDimensionProps,
  ) {
    this.calculatedDimensions[path] = new CalculatedDimension(path, props);
    this.dimensionsIndex[path] = {
      dimension: path,
    };
    return this as unknown as Repository<
      TContext,
      TModelNames,
      TDimensions & {
        [k in TCalculatedDimensionName]: TCalculatedDimensionProps["type"];
      },
      TMetrics,
      TPrivateMembers,
      TFilters,
      THierarchies
    >;
  }

  withCalculatedMetric<
    TCalculatedMetricName extends string,
    TCalculatedMetricProps extends CalculatedMetricProps<
      TContext,
      TModelNames,
      keyof TDimensions & string,
      keyof TMetrics & string
    >,
  >(
    path: Exclude<TCalculatedMetricName, keyof TMetrics | keyof TMetrics>,
    props: TCalculatedMetricProps,
  ) {
    this.calculatedMetrics[path] = new CalculatedMetric(path, props);
    this.metricsIndex[path] = {
      metric: path,
    };
    return this as unknown as Repository<
      TContext,
      TModelNames,
      TDimensions,
      TMetrics & {
        [k in TCalculatedMetricName]: TCalculatedMetricProps["type"];
      },
      TPrivateMembers,
      TFilters,
      THierarchies
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
  withCategoricalHierarchy<THierarchyName extends string>(
    hierarchyName: Exclude<THierarchyName, THierarchies>,
    builder: (args: {
      element: ReturnType<
        typeof makeHierarchyElementInitMaker<Omit<TDimensions, TPrivateMembers>>
      >;
    }) => [AnyHierarchyElement, ...AnyHierarchyElement[]],
  ): Repository<
    TContext,
    TModelNames,
    TDimensions,
    TMetrics,
    TPrivateMembers,
    TFilters,
    THierarchies | THierarchyName
  > {
    const elements = builder({
      element: makeHierarchyElementInitMaker(),
    });
    return this.unsafeWithHierarchy(hierarchyName, elements, "categorical");
  }
  withTemporalHierarchy<THierarchyName extends string>(
    hierarchyName: Exclude<THierarchyName, THierarchies>,
    builder: (args: {
      element: ReturnType<
        typeof makeHierarchyElementInitMaker<Omit<TDimensions, TPrivateMembers>>
      >;
    }) => [AnyHierarchyElement, ...AnyHierarchyElement[]],
  ): Repository<
    TContext,
    TModelNames,
    TDimensions,
    TMetrics,
    TPrivateMembers,
    TFilters,
    THierarchies | THierarchyName
  > {
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
      TContext,
      TModelNames,
      TDimensions,
      TMetrics,
      TPrivateMembers,
      GetFilterFragmentBuilderRegistryPayload<T>,
      THierarchies
    >;
  }

  getFilterFragmentBuilderRegistry() {
    return this.filterFragmentBuilderRegistry;
  }

  join<
    N1 extends TModelNames,
    N2 extends TModelNames & Exclude<TModelNames, N1>,
  >(
    type: AnyJoin["type"],
    modelName1: N1,
    modelName2: N2,
    joinSqlDefFn: JoinFn<TContext, string & keyof TDimensions, N1, N2>,
    opts?: JoinOptions,
  ) {
    const model1 = this.models[modelName1];
    const model2 = this.models[modelName2];

    invariant(model1, `Model ${model1} not found in repository`);
    invariant(model2, `Model ${model2} not found in repository`);
    invariant(
      model1.name !== model2.name,
      `Model ${model1.name} cannot be joined to itself`,
    );

    const joinSqlDef = (context: TContext) => {
      const models = [model1, model2].reduce(
        (acc, model) => {
          acc[model.name as N1 | N2] = makeModelJoinPayload(model, context);
          return acc;
        },
        {} as JoinFnModels<string & keyof TDimensions, N1 | N2>,
      );

      return joinSqlDefFn({
        sql: (strings, ...values) => new SqlFn([...strings], values),
        identifier: (name) => new IdentifierRef(name),
        models,
        getContext: () => context,
      });
    };

    const reversedType = REVERSED_JOIN[type];
    const priority = opts?.priority ?? "normal";

    this.graph.setEdge(model1.name, model2.name, JOIN_WEIGHTS[priority][type]);
    this.graph.setEdge(
      model2.name,
      model1.name,
      JOIN_WEIGHTS[priority][reversedType],
    );

    this.joins[model1.name] ||= {};
    this.joins[model1.name]![model2.name] = {
      left: model1.name,
      right: model2.name,
      joinOnDef: joinSqlDef,
      type: type,
      reversed: false,
      joinType: opts?.type,
    };
    this.joins[model2.name] ||= {};
    this.joins[model2.name]![model1.name] = {
      left: model2.name,
      right: model1.name,
      joinOnDef: joinSqlDef,
      type: reversedType,
      reversed: true,
      joinType: opts?.type,
    };
    return this;
  }

  joinOneToOne<
    N1 extends TModelNames,
    N2 extends TModelNames & Exclude<TModelNames, N1>,
  >(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<TContext, string & keyof TDimensions, N1, N2>,
    opts?: JoinOptions,
  ) {
    return this.join("oneToOne", model1, model2, joinSqlDefFn, opts);
  }

  joinOneToMany<
    N1 extends TModelNames,
    N2 extends TModelNames & Exclude<TModelNames, N1>,
  >(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<TContext, string & keyof TDimensions, N1, N2>,
    opts?: JoinOptions,
  ) {
    return this.join("oneToMany", model1, model2, joinSqlDefFn, opts);
  }

  joinManyToOne<
    N1 extends TModelNames,
    N2 extends TModelNames & Exclude<TModelNames, N1>,
  >(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<TContext, string & keyof TDimensions, N1, N2>,
    opts?: JoinOptions,
  ) {
    return this.join("manyToOne", model1, model2, joinSqlDefFn, opts);
  }

  joinManyToMany<
    N1 extends TModelNames,
    N2 extends TModelNames & Exclude<TModelNames, N1>,
  >(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<TContext, string & keyof TDimensions, N1, N2>,
    opts?: JoinOptions,
  ) {
    return this.join("manyToMany", model1, model2, joinSqlDefFn, opts);
  }

  getDimension(dimensionName: string): Dimension {
    const dimensionIndexEntry = this.dimensionsIndex[dimensionName];
    invariant(dimensionIndexEntry, `Dimension ${dimensionName} not found`);

    const { model: modelName, dimension } = dimensionIndexEntry;
    if (modelName) {
      const model = this.models[modelName];
      invariant(model, `Model ${modelName} not found`);
      return model.getDimension(dimension);
    }

    const calculatedDimension =
      this.calculatedDimensions[dimensionIndexEntry.dimension];
    invariant(
      calculatedDimension,
      `Calculated dimension ${dimensionName} not found`,
    );
    return calculatedDimension;
  }

  getMetric(metricName: string): Metric {
    const metricIndexEntry = this.metricsIndex[metricName];
    invariant(metricIndexEntry, `Metric ${metricName} not found`);
    const { model: modelName, metric } = metricIndexEntry;
    if (modelName) {
      const model = this.models[modelName];
      invariant(model, `Model ${modelName} not found`);
      return model.getMetric(metric);
    }

    const calculatedMetric = this.calculatedMetrics[metricIndexEntry.metric];
    invariant(calculatedMetric, `Calculated dimension ${metricName} not found`);
    return calculatedMetric;
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
    const basicDimension = Object.values(this.models).flatMap((m) =>
      m.getDimensions(),
    );
    const calculatedDimension = Object.values(this.calculatedDimensions);
    return [...basicDimension, ...calculatedDimension];
  }

  getMetrics(): Metric[] {
    const basicMetric = Object.values(this.models).flatMap((m) =>
      m.getMetrics(),
    );
    const calculatedMetric = Object.values(this.calculatedMetrics);
    return [...basicMetric, ...calculatedMetric];
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
    return new QueryBuilder<
      TContext,
      Omit<TDimensions, TPrivateMembers>,
      Omit<TMetrics, TPrivateMembers>,
      Exclude<string & (keyof TDimensions | keyof TMetrics), TPrivateMembers>,
      TFilters,
      P,
      THierarchies
    >(this, dialect);
  }
}

export function repository<C = undefined>() {
  return new Repository<C>();
}
