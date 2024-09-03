import * as semanticLayer from "../index.js";

import { describe, expectTypeOf, it } from "vitest";

import { Simplify } from "type-fest";

const customersModel = semanticLayer
  .model<{ foo: string }>()
  .withName("customers")
  .fromTable("Customer")
  .withDimension("customer_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model, sql }) => sql`${model.column("CustomerId")}`,
  })
  .withDimension("first_name", {
    type: "string",
    sql: ({ model }) => model.column("FirstName"),
  })
  .withMetric("count", {
    type: "number",
    sql: ({ model, sql }) => sql`COUNT(DISTINCT ${model.column("CustomerId")})`,
  })
  .withCategoricalHierarchy("customerHierarchy1", ({ element }) => [
    element("customer")
      .withDimensions(["customer_id", "first_name"])
      .withKey(["customer_id"])
      .withFormat(
        ["first_name"],
        ({ dimension }) => `${dimension("first_name")}`,
      ),
  ])
  .withCategoricalHierarchy("customerHierarchy2", ({ element }) => [
    element("customer")
      .withDimensions(["customer_id", "first_name"])
      .withKey(["customer_id"])
      .withFormat(
        ["first_name"],
        ({ dimension }) => `${dimension("first_name")}`,
      ),
  ]);

const repository = semanticLayer
  .repository<{ foo: string }>()
  .withModel(customersModel);

const queryBuilder = repository.build("postgresql");

describe("model", () => {
  type CustomersModel = typeof customersModel;
  type ModelWithDimension = typeof customersModel.withDimension;
  type ModelWithMetric = typeof customersModel.withMetric;
  type ModelWithCategoricalHierarchy =
    typeof customersModel.withCategoricalHierarchy;
  type ModelWithTemporalHierarchy = typeof customersModel.withTemporalHierarchy;

  expectTypeOf<
    semanticLayer.ModelWithMatchingContext<{ foo: string }, CustomersModel>
  >().toEqualTypeOf<CustomersModel>();

  expectTypeOf<
    semanticLayer.ModelWithMatchingContext<{ bar: "number" }, CustomersModel>
  >().toEqualTypeOf<never>();
  it("can type check model generics", () => {
    expectTypeOf<
      semanticLayer.GetModelContext<CustomersModel>
    >().toEqualTypeOf<{
      foo: string;
    }>();

    expectTypeOf<
      semanticLayer.GetModelName<CustomersModel>
    >().toEqualTypeOf<"customers">();

    expectTypeOf<
      semanticLayer.GetModelDimensions<CustomersModel>
    >().toEqualTypeOf<{
      "customers.customer_id": "number";
      "customers.first_name": "string";
    }>();

    expectTypeOf<
      semanticLayer.GetModelMetrics<CustomersModel>
    >().toEqualTypeOf<{
      "customers.count": "number";
    }>();

    expectTypeOf<
      semanticLayer.GetModelHierarchies<CustomersModel>
    >().toEqualTypeOf<"customerHierarchy1" | "customerHierarchy2">();
  });

  it("can type check MemberFormat", () => {
    expectTypeOf<semanticLayer.MemberFormat<"number">>().toEqualTypeOf<
      "percentage" | "currency" | ((value: number | null | undefined) => string)
    >;

    expectTypeOf<semanticLayer.MemberFormat<"boolean">>().toEqualTypeOf<
      | "percentage"
      | "currency"
      | ((value: boolean | null | undefined) => string)
    >;

    expectTypeOf<semanticLayer.MemberFormat<"date">>().toEqualTypeOf<
      "percentage" | "currency" | ((value: Date | null | undefined) => string)
    >;

    expectTypeOf<semanticLayer.MemberFormat<"datetime">>().toEqualTypeOf<
      "percentage" | "currency" | ((value: Date | null | undefined) => string)
    >;

    expectTypeOf<semanticLayer.MemberFormat<"time">>().toEqualTypeOf<
      "percentage" | "currency" | ((value: string | null | undefined) => string)
    >;
  });

  it("can type check model.withDimension", () => {
    expectTypeOf<Parameters<ModelWithDimension>[0]>().toEqualTypeOf<string>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithDimension>[1], { type: "string" }>>
    >().toEqualTypeOf<{
      type: "string";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"string"> | undefined;
      sql?:
        | semanticLayer.BasicDimensionSqlFn<
            {
              foo: string;
            },
            "customer_id" | "first_name"
          >
        | undefined;
      primaryKey?: boolean | undefined;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithDimension>[1], { type: "number" }>>
    >().toEqualTypeOf<{
      type: "number";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"number"> | undefined;
      sql?:
        | semanticLayer.BasicDimensionSqlFn<
            {
              foo: string;
            },
            "customer_id" | "first_name"
          >
        | undefined;
      primaryKey?: boolean | undefined;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithDimension>[1], { type: "boolean" }>>
    >().toEqualTypeOf<{
      type: "boolean";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"boolean"> | undefined;
      sql?:
        | semanticLayer.BasicDimensionSqlFn<
            {
              foo: string;
            },
            "customer_id" | "first_name"
          >
        | undefined;
      primaryKey?: boolean | undefined;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithDimension>[1], { type: "datetime" }>>
    >().toEqualTypeOf<{
      type: "datetime";
      description?: string | undefined;
      omitGranularity?: boolean | undefined;
      format?: semanticLayer.MemberFormat<"datetime"> | undefined;
      sql?:
        | semanticLayer.BasicDimensionSqlFn<
            {
              foo: string;
            },
            "customer_id" | "first_name"
          >
        | undefined;
      primaryKey?: boolean | undefined;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithDimension>[1], { type: "date" }>>
    >().toEqualTypeOf<{
      type: "date";
      description?: string | undefined;
      omitGranularity?: boolean | undefined;
      format?: semanticLayer.MemberFormat<"date"> | undefined;
      sql?:
        | semanticLayer.BasicDimensionSqlFn<
            {
              foo: string;
            },
            "customer_id" | "first_name"
          >
        | undefined;
      primaryKey?: boolean | undefined;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithDimension>[1], { type: "time" }>>
    >().toEqualTypeOf<{
      type: "time";
      description?: string | undefined;
      omitGranularity?: boolean | undefined;
      format?: semanticLayer.MemberFormat<"time"> | undefined;
      sql?:
        | semanticLayer.BasicDimensionSqlFn<
            {
              foo: string;
            },
            "customer_id" | "first_name"
          >
        | undefined;
      primaryKey?: boolean | undefined;
    }>();

    expectTypeOf<Parameters<ModelWithDimension>[1]["type"]>().toEqualTypeOf<
      "string" | "number" | "boolean" | "date" | "datetime" | "time"
    >();

    expectTypeOf<
      semanticLayer.BasicDimensionSqlFn<
        {
          foo: string;
        },
        "customer_id" | "first_name"
      >
    >().toEqualTypeOf<
      (args: {
        identifier: (name: string) => semanticLayer.IdentifierRef;
        model: {
          column: (name: string) => semanticLayer.ColumnRef;
          dimension: (
            name: "customer_id" | "first_name",
          ) => semanticLayer.DimensionRef;
        };
        sql: (
          strings: TemplateStringsArray,
          ...values: unknown[]
        ) => semanticLayer.SqlFn;
        getContext: () => { foo: string };
      }) => semanticLayer.Ref
    >();
  });

  it("can type check model.withMetric", () => {
    expectTypeOf<Parameters<ModelWithMetric>[0]>().toEqualTypeOf<string>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithMetric>[1], { type: "string" }>>
    >().toEqualTypeOf<{
      type: "string";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"string"> | undefined;
      sql: semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        "customer_id" | "first_name",
        "count"
      >;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithMetric>[1], { type: "number" }>>
    >().toEqualTypeOf<{
      type: "number";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"number"> | undefined;
      sql: semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        "customer_id" | "first_name",
        "count"
      >;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithMetric>[1], { type: "boolean" }>>
    >().toEqualTypeOf<{
      type: "boolean";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"boolean"> | undefined;
      sql: semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        "customer_id" | "first_name",
        "count"
      >;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithMetric>[1], { type: "datetime" }>>
    >().toEqualTypeOf<{
      type: "datetime";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"datetime"> | undefined;
      sql: semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        "customer_id" | "first_name",
        "count"
      >;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithMetric>[1], { type: "date" }>>
    >().toEqualTypeOf<{
      type: "date";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"date"> | undefined;
      sql: semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        "customer_id" | "first_name",
        "count"
      >;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithMetric>[1], { type: "time" }>>
    >().toEqualTypeOf<{
      type: "time";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"time"> | undefined;
      sql: semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        "customer_id" | "first_name",
        "count"
      >;
    }>();

    expectTypeOf<Parameters<ModelWithMetric>[1]["type"]>().toEqualTypeOf<
      "string" | "number" | "boolean" | "date" | "datetime" | "time"
    >();

    expectTypeOf<
      semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        "customer_id" | "first_name",
        "count"
      >
    >().toEqualTypeOf<
      (args: {
        identifier: (name: string) => semanticLayer.IdentifierRef;
        model: {
          column: (
            name: string,
          ) => semanticLayer.MetricAliasColumnOrDimensionRef<semanticLayer.ColumnRef>;
          dimension: (
            name: "customer_id" | "first_name",
          ) => semanticLayer.MetricAliasColumnOrDimensionRef<semanticLayer.DimensionRef>;
          metric: (name: "count") => semanticLayer.MetricAliasMetricRef;
        };
        sql: (
          strings: TemplateStringsArray,
          ...values: unknown[]
        ) => semanticLayer.SqlFn;
        getContext: () => { foo: string };
      }) => semanticLayer.SqlFn
    >();
  });

  it("can type check model.withCategoricalHierarchy", () => {
    expectTypeOf<
      Parameters<ModelWithCategoricalHierarchy>[0]
    >().toEqualTypeOf<string>();

    expectTypeOf<Parameters<ModelWithCategoricalHierarchy>[1]>().toEqualTypeOf<
      (args: {
        element: {
          (
            name: string,
          ): semanticLayer.HierarchyElementInit<{
            customer_id: "number";
            first_name: "string";
          }>;
          fromDimension<DN extends "customer_id" | "first_name">(
            name: DN,
          ): semanticLayer.HierarchyElement<
            {
              customer_id: "number";
              first_name: "string";
            },
            DN
          >;
        };
      }) => [
        semanticLayer.AnyHierarchyElement,
        ...semanticLayer.AnyHierarchyElement[],
      ]
    >();
  });

  it("can type check model.withTemporalHierarchy", () => {
    expectTypeOf<
      Parameters<ModelWithTemporalHierarchy>[0]
    >().toEqualTypeOf<string>();

    expectTypeOf<Parameters<ModelWithTemporalHierarchy>[1]>().toEqualTypeOf<
      (args: {
        element: {
          (
            name: string,
          ): semanticLayer.HierarchyElementInit<{
            customer_id: "number";
            first_name: "string";
          }>;
          fromDimension<DN extends "customer_id" | "first_name">(
            name: DN,
          ): semanticLayer.HierarchyElement<
            {
              customer_id: "number";
              first_name: "string";
            },
            DN
          >;
        };
      }) => [
        semanticLayer.AnyHierarchyElement,
        ...semanticLayer.AnyHierarchyElement[],
      ]
    >();
  });
});

describe("repository", () => {
  type Repository = typeof repository;
  type RepositoryWithCalculatedDimension =
    typeof repository.withCalculatedDimension;
  type RepositoryWithCalculatedMetric = typeof repository.withCalculatedMetric;
  type RepositoryWithCategoricalHierarchy =
    typeof repository.withCategoricalHierarchy;
  type RepositoryWithTemporalHierarchy =
    typeof repository.withTemporalHierarchy;
  it("can type check repository generics", () => {
    type GetRepositoryContext<T> = T extends semanticLayer.Repository<
      infer TContext,
      any,
      any,
      any,
      any,
      any
    >
      ? TContext
      : never;

    type GetRepositoryModelNames<T> = T extends semanticLayer.Repository<
      any,
      infer TModelNames,
      any,
      any,
      any,
      any
    >
      ? TModelNames
      : never;

    type GetRepositoryDimensions<T> = T extends semanticLayer.Repository<
      any,
      any,
      infer TDimensions,
      any,
      any,
      any
    >
      ? TDimensions
      : never;

    type GetRepositoryMetrics<T> = T extends semanticLayer.Repository<
      any,
      any,
      any,
      infer TMetrics,
      any,
      any
    >
      ? TMetrics
      : never;

    type GetRepositoryHierarchies<T> = T extends semanticLayer.Repository<
      any,
      any,
      any,
      any,
      any,
      infer THierarchies
    >
      ? THierarchies
      : never;
    expectTypeOf<GetRepositoryContext<Repository>>().toEqualTypeOf<{
      foo: string;
    }>();

    expectTypeOf<
      GetRepositoryModelNames<Repository>
    >().toEqualTypeOf<"customers">();

    expectTypeOf<GetRepositoryDimensions<Repository>>().branded.toEqualTypeOf<{
      "customers.customer_id": "number";
      "customers.first_name": "string";
    }>();

    expectTypeOf<GetRepositoryMetrics<Repository>>().branded.toEqualTypeOf<{
      "customers.count": "number";
    }>();

    expectTypeOf<GetRepositoryHierarchies<Repository>>().toEqualTypeOf<
      "customers.customerHierarchy1" | "customers.customerHierarchy2"
    >();
  });

  it("can type check repository.withCalculatedDimension", () => {
    expectTypeOf<
      Parameters<RepositoryWithCalculatedDimension>[0]
    >().toEqualTypeOf<string>();

    expectTypeOf<
      Simplify<
        Extract<
          Parameters<RepositoryWithCalculatedDimension>[1],
          { type: "string" }
        >
      >
    >().toEqualTypeOf<{
      type: "string";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"string"> | undefined;
      sql: semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name"
      >;
    }>();

    expectTypeOf<
      Simplify<
        Extract<
          Parameters<RepositoryWithCalculatedDimension>[1],
          { type: "number" }
        >
      >
    >().toEqualTypeOf<{
      type: "number";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"number"> | undefined;
      sql: semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name"
      >;
    }>();

    expectTypeOf<
      Simplify<
        Extract<
          Parameters<RepositoryWithCalculatedDimension>[1],
          { type: "number" }
        >
      >
    >().toEqualTypeOf<{
      type: "number";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"number"> | undefined;
      sql: semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name"
      >;
    }>();

    expectTypeOf<
      Simplify<
        Extract<
          Parameters<RepositoryWithCalculatedDimension>[1],
          { type: "date" }
        >
      >
    >().toEqualTypeOf<{
      type: "date";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"date"> | undefined;
      sql: semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name"
      >;
    }>();

    expectTypeOf<
      Simplify<
        Extract<
          Parameters<RepositoryWithCalculatedDimension>[1],
          { type: "datetime" }
        >
      >
    >().toEqualTypeOf<{
      type: "datetime";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"datetime"> | undefined;
      sql: semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name"
      >;
    }>();

    expectTypeOf<
      Simplify<
        Extract<
          Parameters<RepositoryWithCalculatedDimension>[1],
          { type: "time" }
        >
      >
    >().toEqualTypeOf<{
      type: "time";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"time"> | undefined;
      sql: semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name"
      >;
    }>();

    expectTypeOf<
      semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name"
      >
    >().toEqualTypeOf<
      (args: {
        identifier: (name: string) => semanticLayer.IdentifierRef;
        models: {
          customers: {
            dimension: (
              dimensionName: "customer_id" | "first_name",
            ) => semanticLayer.DimensionRef;
            column: (columnName: string) => semanticLayer.ColumnRef;
          };
        };
        sql: (
          strings: TemplateStringsArray,
          ...values: unknown[]
        ) => semanticLayer.SqlFn;
        getContext: () => {
          foo: string;
        };
      }) => semanticLayer.SqlFn
    >();
  });

  it("can type check repository.withCalculatedMetric", () => {
    expectTypeOf<
      Parameters<RepositoryWithCalculatedMetric>[0]
    >().toEqualTypeOf<string>();

    expectTypeOf<
      Simplify<
        Extract<
          Parameters<RepositoryWithCalculatedMetric>[1],
          { type: "string" }
        >
      >
    >().toEqualTypeOf<{
      type: "string";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"string"> | undefined;
      sql: semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name",
        "customers.count"
      >;
    }>();

    expectTypeOf<
      Simplify<
        Extract<
          Parameters<RepositoryWithCalculatedMetric>[1],
          { type: "number" }
        >
      >
    >().toEqualTypeOf<{
      type: "number";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"number"> | undefined;
      sql: semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name",
        "customers.count"
      >;
    }>();

    expectTypeOf<
      Simplify<
        Extract<
          Parameters<RepositoryWithCalculatedMetric>[1],
          { type: "number" }
        >
      >
    >().toEqualTypeOf<{
      type: "number";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"number"> | undefined;
      sql: semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name",
        "customers.count"
      >;
    }>();

    expectTypeOf<
      Simplify<
        Extract<Parameters<RepositoryWithCalculatedMetric>[1], { type: "date" }>
      >
    >().toEqualTypeOf<{
      type: "date";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"date"> | undefined;
      sql: semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name",
        "customers.count"
      >;
    }>();

    expectTypeOf<
      Simplify<
        Extract<
          Parameters<RepositoryWithCalculatedMetric>[1],
          { type: "datetime" }
        >
      >
    >().toEqualTypeOf<{
      type: "datetime";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"datetime"> | undefined;
      sql: semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name",
        "customers.count"
      >;
    }>();

    expectTypeOf<
      Simplify<
        Extract<Parameters<RepositoryWithCalculatedMetric>[1], { type: "time" }>
      >
    >().toEqualTypeOf<{
      type: "time";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"time"> | undefined;
      sql: semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name",
        "customers.count"
      >;
    }>();

    expectTypeOf<
      semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        "customers.customer_id" | "customers.first_name",
        "customers.count"
      >
    >().toEqualTypeOf<
      (args: {
        identifier: (name: string) => semanticLayer.IdentifierRef;
        models: {
          customers: {
            metric: (metricName: "count") => semanticLayer.MetricAliasMetricRef;
            dimension: (
              dimensionName: "customer_id" | "first_name",
            ) => semanticLayer.MetricAliasColumnOrDimensionRef<semanticLayer.DimensionRef>;
            column: (
              columnName: string,
            ) => semanticLayer.MetricAliasColumnOrDimensionRef<semanticLayer.ColumnRef>;
          };
        };
        sql: (
          strings: TemplateStringsArray,
          ...values: unknown[]
        ) => semanticLayer.SqlFn;
        getContext: () => {
          foo: string;
        };
      }) => semanticLayer.SqlFn
    >();
  });

  it("can type check repository.withCategoricalHierarchy", () => {
    expectTypeOf<
      Parameters<RepositoryWithCategoricalHierarchy>[0]
    >().toEqualTypeOf<string>();

    expectTypeOf<
      Parameters<RepositoryWithCategoricalHierarchy>[1]
    >().toEqualTypeOf<
      (args: {
        element: {
          (
            name: string,
          ): semanticLayer.HierarchyElementInit<{
            "customers.customer_id": "number";
            "customers.first_name": "string";
          }>;
          fromDimension<
            DN extends "customers.customer_id" | "customers.first_name",
          >(
            name: DN,
          ): semanticLayer.HierarchyElement<
            {
              "customers.customer_id": "number";
              "customers.first_name": "string";
            },
            DN
          >;
        };
      }) => [
        semanticLayer.AnyHierarchyElement,
        ...semanticLayer.AnyHierarchyElement[],
      ]
    >();
  });

  it("can type check repository.withTemporalHierarchy", () => {
    expectTypeOf<
      Parameters<RepositoryWithTemporalHierarchy>[0]
    >().toEqualTypeOf<string>();

    expectTypeOf<
      Parameters<RepositoryWithTemporalHierarchy>[1]
    >().toEqualTypeOf<
      (args: {
        element: {
          (
            name: string,
          ): semanticLayer.HierarchyElementInit<{
            "customers.customer_id": "number";
            "customers.first_name": "string";
          }>;
          fromDimension<
            DN extends "customers.customer_id" | "customers.first_name",
          >(
            name: DN,
          ): semanticLayer.HierarchyElement<
            {
              "customers.customer_id": "number";
              "customers.first_name": "string";
            },
            DN
          >;
        };
      }) => [
        semanticLayer.AnyHierarchyElement,
        ...semanticLayer.AnyHierarchyElement[],
      ]
    >();
  });
});

describe("query builder", () => {
  it("can type check query builder query", () => {
    type Q = semanticLayer.QueryBuilderQuery<typeof queryBuilder>;
    type QF = Simplify<NonNullable<Q["filters"]>[number]>;

    expectTypeOf<Q["members"][number]>().toEqualTypeOf<
      "customers.customer_id" | "customers.first_name" | "customers.count"
    >();

    expectTypeOf<NonNullable<Q["order"]>[number]>().toEqualTypeOf<{
      member:
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.count";
      direction: "asc" | "desc";
    }>();

    expectTypeOf<Extract<QF, { operator: "equals" }>>().toEqualTypeOf<{
      operator: "equals";
      member:
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.count";
      value: (string | number | bigint | boolean | Date)[];
    }>();
  });

  it("can type check query builder result type inference", () => {
    const query = queryBuilder.buildQuery(
      {
        members: ["customers.customer_id", "customers.first_name"],
      },
      { foo: "" },
    );

    expectTypeOf<
      semanticLayer.InferSqlQueryResultType<typeof query>
    >().toEqualTypeOf<{
      customers___customer_id: number;
      customers___first_name: string;
    }>();
  });
});
