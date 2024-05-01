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
import { AvailableDialects, MemberNameToType } from "./types.js";

import graphlib from "@dagrejs/graphlib";
import invariant from "tiny-invariant";
import { AnsiDialect } from "./dialect/ansi.js";
import { BaseDialect } from "./dialect/base.js";
import { DatabricksDialect } from "./dialect/databricks.js";
import { PostgresqlDialect } from "./dialect/postgresql.js";
import { QueryBuilder } from "./query-builder.js";

// biome-ignore lint/suspicious/noExplicitAny: Using any for inference
export type ModelC<T> = T extends Model<infer C, any, any, any> ? C : never;

// biome-ignore lint/suspicious/noExplicitAny: Using any for inference
export type ModelN<T> = T extends Model<any, infer N, any, any> ? N : never;
// biome-ignore lint/suspicious/noExplicitAny: Using any for inference
export type ModelD<T> = T extends Model<any, infer N, infer D, any>
  ? { [K in string & keyof D as `${N}.${K}`]: D[K] }
  : never;
// biome-ignore lint/suspicious/noExplicitAny: Using any for inference
export type ModelM<T> = T extends Model<any, infer N, any, infer M>
  ? { [K in string & keyof M as `${N}.${K}`]: M[K] }
  : never;

export type ModelWithMatchingContext<C, T extends AnyModel> = [C] extends [
  ModelC<T>,
]
  ? T
  : never;

// biome-ignore lint/suspicious/noExplicitAny: Using any for inference
export type AnyRepository = Repository<any, any, any, any>;

function getDialect(dialect: AvailableDialects): BaseDialect {
  switch (dialect) {
    case "ansi":
      return new AnsiDialect();
    case "postgresql":
      return new PostgresqlDialect();
    case "databricks":
      return new DatabricksDialect();
    default:
      // biome-ignore lint/correctness/noSwitchDeclarations: <explanation>
      const _exhaustiveCheck: never = dialect;
      throw new Error(`Dialect ${dialect} not supported`);
  }
}

export class Repository<
  C,
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
  readonly joins: Record<string, Record<string, AnyJoin>> = {};
  readonly graph: graphlib.Graph = new graphlib.Graph();
  readonly dimensionsIndex: Record<
    string,
    { model: string; dimension: string }
  > = {} as Record<string, { model: string; dimension: string }>;
  readonly metricsIndex: Record<string, { model: string; metric: string }> =
    {} as Record<string, { model: string; metric: string }>;

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
      F
    >;
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
      GetFilterFragmentBuilderRegistryPayload<T>
    >;
  }

  getFilterFragmentBuilderRegistry() {
    return this.filterFragmentBuilderRegistry;
  }

  join<N1 extends string, N2 extends string>(
    type: AnyJoin["type"],
    modelName1: N1,
    modelName2: N2,
    joinSqlDefFn: JoinFn<C, string & keyof D, N1, N2>,
  ) {
    const model1 = this.models[modelName1];
    const model2 = this.models[modelName2];

    invariant(model1, `Model ${model1} not found in repository`);
    invariant(model2, `Model ${model2} not found in repository`);

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

  joinOneToOne<N1 extends string, N2 extends string>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<C, string & keyof D, N1, N2>,
  ) {
    return this.join("oneToOne", model1, model2, joinSqlDefFn);
  }

  joinOneToMany<N1 extends string, N2 extends string>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<C, string & keyof D, N1, N2>,
  ) {
    return this.join("oneToMany", model1, model2, joinSqlDefFn);
  }

  joinManyToOne<N1 extends string, N2 extends string>(
    model1: N1,
    model2: N2,
    joinSqlDefFn: JoinFn<C, string & keyof D, N1, N2>,
  ) {
    return this.join("manyToOne", model1, model2, joinSqlDefFn);
  }

  joinManyToMany<N1 extends string, N2 extends string>(
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

  build(dialectName: AvailableDialects) {
    const dialect = getDialect(dialectName);
    return new QueryBuilder<C, D, M, F>(this, dialect);
  }
}

export function repository<C = undefined>() {
  return new Repository<C>();
}
