import * as queryBuilder from "../query/builder.js";

import { FilterType, Query } from "../../types.js";
import {
  AnyFilterFragmentBuilderRegistry,
  GetFilterFragmentBuilderRegistryPayload,
  defaultFilterFragmentBuilderRegistry,
} from "../query/filter-builder.js";
import { AnyTable, Table } from "./table.js";

import graphlib from "@dagrejs/graphlib";
import invariant from "tiny-invariant";
import { BaseDialect } from "../dialect/base.js";

// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type TableTN<T> = T extends Table<infer TN, any, any> ? TN : never;
// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type TableDN<T> = T extends Table<infer TN, infer DN, any>
  ? `${TN}.${DN}`
  : never;
// biome-ignore lint/suspicious/noExplicitAny: <explanation>
export type TableMN<T> = T extends Table<infer TN, any, infer MN>
  ? `${TN}.${MN}`
  : never;

export class JoinDimensionRef<TN extends string, DN extends string> {
  constructor(
    private readonly table: TN,
    private readonly dimension: DN,
  ) {}
  render(database: Database, dialect: BaseDialect) {
    return database
      .getTable(this.table)
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
  TN1 extends string,
  TN2 extends string,
> = (args: {
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => JoinOnDef;
  dimensions: JoinDimensions<DN, TN1, TN2>;
}) => JoinOnDef;

export type TableDimensionsWithoutTablePrefix<
  TN extends string,
  DN extends string,
> = DN extends `${TN}.${infer D}` ? D : never;

export type JoinDimensions<
  DN extends string,
  TN1 extends string,
  TN2 extends string,
> = {
  [TK in TN1]: {
    [DK in TableDimensionsWithoutTablePrefix<TN1, DN>]: JoinDimensionRef<
      TK,
      DK
    >;
  };
} & {
  [TK in TN2]: {
    [DK in TableDimensionsWithoutTablePrefix<TN2, DN>]: JoinDimensionRef<
      TK,
      DK
    >;
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
  TN extends string = never,
  DN extends string = never,
  MN extends string = never,
  F = GetFilterFragmentBuilderRegistryPayload<
    ReturnType<typeof defaultFilterFragmentBuilderRegistry>
  >,
> {
  private readonly tables: Record<string, AnyTable> = {};
  private filterFragmentBuilderRegistry: AnyFilterFragmentBuilderRegistry =
    defaultFilterFragmentBuilderRegistry();
  readonly joins: Record<string, Record<string, Join>> = {};
  readonly graph: graphlib.Graph = new graphlib.Graph();
  readonly dimensionsIndex: Record<
    string,
    { table: string; dimension: string }
  > = {} as Record<string, { table: string; dimension: string }>;
  readonly metricsIndex: Record<string, { table: string; metric: string }> =
    {} as Record<string, { table: string; metric: string }>;

  public withTable<T extends AnyTable>(table: T) {
    this.tables[table.name] = table;
    for (const dimension in table.dimensions) {
      this.dimensionsIndex[`${table.name}.${dimension}`] = {
        table: table.name,
        dimension,
      };
    }
    for (const metric in table.metrics) {
      this.metricsIndex[`${table.name}.${metric}`] = {
        table: table.name,
        metric,
      };
    }
    return this as Database<
      TN | TableTN<T>,
      DN | TableDN<T>,
      MN | TableMN<T>,
      F
    >;
  }

  public withFilterFragmentBuilderRegistry<
    T extends AnyFilterFragmentBuilderRegistry,
  >(filterFragmentBuilderRegistry: T) {
    this.filterFragmentBuilderRegistry = filterFragmentBuilderRegistry;
    return this as Database<
      TN,
      DN,
      MN,
      GetFilterFragmentBuilderRegistryPayload<T>
    >;
  }

  getFilterBuilder(
    database: Database,
    dialect: BaseDialect,
    filterType: FilterType,
    referencedTables: string[],
    metricPrefixes?: Record<string, string>,
  ) {
    return this.filterFragmentBuilderRegistry.getFilterBuilder(
      database,
      dialect,
      filterType,
      referencedTables,
      metricPrefixes,
    );
  }

  join<TN1 extends string, TN2 extends string>(
    type: Join["type"],
    tableName1: TN1,
    tableName2: TN2,
    joinSqlDefFn: JoinFn<DN, TN1, TN2>,
  ) {
    const table1 = this.tables[tableName1];
    const table2 = this.tables[tableName2];
    invariant(table1, `Table ${table1} not found in database`);
    invariant(table2, `Table ${table2} not found in database`);
    const dimensions = {
      [table1.name]: Object.keys(table1.dimensions).reduce<
        Record<string, JoinDimensionRef<string, string>>
      >((acc, dimension) => {
        acc[dimension] = new JoinDimensionRef(tableName1, dimension);
        return acc;
      }, {}),
      [table2.name]: Object.keys(table2.dimensions).reduce<
        Record<string, JoinDimensionRef<string, string>>
      >((acc, dimension) => {
        acc[dimension] = new JoinDimensionRef(table2.name, dimension);
        return acc;
      }, {}),
    } as JoinDimensions<DN, TN1, TN2>;

    const joinSqlDef = joinSqlDefFn({
      sql: (strings, ...values) => new JoinOnDef([...strings], values),
      dimensions,
    });

    const reversedType = REVERSED_JOIN[type];

    this.graph.setEdge(table1.name, table2.name, JOIN_WEIGHTS[type]);
    this.graph.setEdge(table2.name, table1.name, JOIN_WEIGHTS[reversedType]);

    this.joins[table1.name] ||= {};
    this.joins[table1.name]![table2.name] = {
      left: table1.name,
      right: table2.name,
      joinOnDef: joinSqlDef,
      type: type,
      reversed: false,
    };
    this.joins[table2.name] ||= {};
    this.joins[table2.name]![table1.name] = {
      left: table2.name,
      right: table1.name,
      joinOnDef: joinSqlDef,
      type: reversedType,
      reversed: true,
    };
    return this;
  }

  joinOneToOne<TN1 extends string, TN2 extends string>(
    table1: TN1,
    table2: TN2,
    joinSqlDefFn: JoinFn<DN, TN1, TN2>,
  ) {
    return this.join("oneToOne", table1, table2, joinSqlDefFn);
  }

  joinOneToMany<TN1 extends string, TN2 extends string>(
    table1: TN1,
    table2: TN2,
    joinSqlDefFn: JoinFn<DN, TN1, TN2>,
  ) {
    return this.join("oneToMany", table1, table2, joinSqlDefFn);
  }

  joinManyToOne<TN1 extends string, TN2 extends string>(
    table1: TN1,
    table2: TN2,
    joinSqlDefFn: JoinFn<DN, TN1, TN2>,
  ) {
    return this.join("manyToOne", table1, table2, joinSqlDefFn);
  }

  joinManyToMany<TN1 extends string, TN2 extends string>(
    table1: TN1,
    table2: TN2,
    joinSqlDefFn: JoinFn<DN, TN1, TN2>,
  ) {
    return this.join("manyToMany", table1, table2, joinSqlDefFn);
  }

  getDimension(dimensionName: string) {
    invariant(
      this.dimensionsIndex[dimensionName],
      `Dimension ${dimensionName} not found`,
    );
    const { table: tableName, dimension } =
      this.dimensionsIndex[dimensionName]!;
    const table = this.tables[tableName];
    if (!table) {
      throw new Error(`Table ${tableName} not found`);
    }
    return table.getDimension(dimension);
  }

  getMetric(metricName: string) {
    invariant(this.metricsIndex[metricName], `Metric ${metricName} not found`);
    const { table: tableName, metric } = this.metricsIndex[metricName]!;
    const table = this.tables[tableName];
    if (!table) {
      throw new Error(`Table ${tableName} not found`);
    }
    return table.getMetric(metric);
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

  getTable(tableName: string) {
    const table = this.tables[tableName];
    if (!table) {
      throw new Error(`Table ${tableName} not found`);
    }
    return table;
  }

  getTableJoins(tableName: string) {
    return Object.values(this.joins[tableName] ?? {});
  }

  getJoin(tableName: string, joinTableName: string) {
    return this.joins[tableName]?.[joinTableName];
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
