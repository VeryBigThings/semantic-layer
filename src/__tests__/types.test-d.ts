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
  .withDimension("created_at", {
    type: "datetime",
    sql: ({ model, sql }) => sql`${model.column("CreatedAt")}`,
  })
  .withDimension("updated_at", {
    type: "datetime",
    sql: ({ model, sql }) => sql`${model.column("UpdatedAt")}`,
    private: true,
  })
  .withDimension("private_dimension", {
    type: "string",
    private: true,
    sql: ({ model }) => model.column("private_dimension"),
  })
  .withMetric("private_metric", {
    type: "number",
    private: true,
    sql: ({ model, sql }) =>
      sql`COUNT(DISTINCT ${model.column("PrivateMetric")})`,
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
  .withTemporalHierarchy("customerHierarchy2", ({ element }) => [
    element("created_at")
      .withDimensions(["created_at"])
      .withKey(["created_at"])
      .withFormat(
        ["created_at"],
        ({ dimension }) => `${dimension("created_at")}`,
      ),
  ]);

const repository = semanticLayer
  .repository<{ foo: string }>()
  .withModel(customersModel)
  .withCategoricalHierarchy("repositoryHierarchy1", ({ element }) => [
    element("customer")
      .withDimensions(["customers.customer_id", "customers.first_name"])
      .withKey(["customers.customer_id"])
      .withFormat(
        ["customers.first_name"],
        ({ dimension }) => `${dimension("customers.first_name")}`,
      ),
  ])
  .withTemporalHierarchy("repositoryHierarchy2", ({ element }) => [
    element("created_at")
      .withDimensions(["customers.created_at"])
      .withKey(["customers.created_at"])
      .withFormat(
        ["customers.created_at"],
        ({ dimension }) => `${dimension("customers.created_at")}`,
      ),
  ]);

const queryBuilder = repository.build("postgresql");

it("can infer result type from query object", () => {
  const query = {
    members: ["customers.customer_id", "customers.first_name"],
    order: [{ member: "customers.customer_id", direction: "asc" }],
    filters: [
      { operator: "equals", member: "customers.customer_id", value: [1] },
    ],
    limit: 10,
  } satisfies semanticLayer.QueryBuilderQuery<typeof queryBuilder>;

  expectTypeOf<
    semanticLayer.InferSqlQueryResultTypeFromQuery<
      typeof queryBuilder,
      typeof query
    >
  >().toEqualTypeOf<{
    customers___customer_id: number;
    customers___first_name: string;
  }>();

  expectTypeOf<
    semanticLayer.InferSqlQueryResultTypeFromQuery<
      typeof queryBuilder,
      typeof query,
      { "customers.customer_id": boolean }
    >
  >().toEqualTypeOf<{
    customers___customer_id: boolean;
    customers___first_name: string;
  }>();

  expectTypeOf<
    semanticLayer.InferSqlQueryResultTypeFromQuery<
      typeof queryBuilder,
      typeof query,
      { customers___customer_id: boolean }
    >
  >().toEqualTypeOf<{
    customers___customer_id: boolean;
    customers___first_name: string;
  }>();
});

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
      "customers.created_at": "datetime";
      "customers.created_at.date": "date";
      "customers.created_at.time": "time";
      "customers.created_at.hour": "string";
      "customers.created_at.year": "number";
      "customers.created_at.quarter": "string";
      "customers.created_at.quarter_of_year": "number";
      "customers.created_at.month": "string";
      "customers.created_at.month_num": "number";
      "customers.created_at.week": "string";
      "customers.created_at.week_num": "number";
      "customers.created_at.day_of_month": "number";
      "customers.created_at.hour_of_day": "number";
      "customers.created_at.minute": "string";
      "customers.updated_at": "datetime";
      "customers.updated_at.date": "date";
      "customers.updated_at.time": "time";
      "customers.updated_at.hour": "string";
      "customers.updated_at.year": "number";
      "customers.updated_at.quarter": "string";
      "customers.updated_at.quarter_of_year": "number";
      "customers.updated_at.month": "string";
      "customers.updated_at.month_num": "number";
      "customers.updated_at.week": "string";
      "customers.updated_at.week_num": "number";
      "customers.updated_at.day_of_month": "number";
      "customers.updated_at.hour_of_day": "number";
      "customers.updated_at.minute": "string";
      "customers.private_dimension": "string";
    }>();

    expectTypeOf<
      semanticLayer.GetModelMetrics<CustomersModel>
    >().toEqualTypeOf<{
      "customers.count": "number";
      "customers.private_metric": "number";
    }>();

    expectTypeOf<
      semanticLayer.GetModelPrivateMembers<CustomersModel>
    >().toEqualTypeOf<
      | "customers.private_dimension"
      | "customers.private_metric"
      | "customers.updated_at"
      | "customers.updated_at.date"
      | "customers.updated_at.time"
      | "customers.updated_at.hour"
      | "customers.updated_at.year"
      | "customers.updated_at.quarter"
      | "customers.updated_at.quarter_of_year"
      | "customers.updated_at.month"
      | "customers.updated_at.month_num"
      | "customers.updated_at.week"
      | "customers.updated_at.week_num"
      | "customers.updated_at.day_of_month"
      | "customers.updated_at.hour_of_day"
      | "customers.updated_at.minute"
    >();

    expectTypeOf<
      semanticLayer.GetModelHierarchies<CustomersModel>
    >().toEqualTypeOf<
      "customerHierarchy1" | "customerHierarchy2" | "created_at"
    >();
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
      private?: boolean | undefined;
      sql?:
        | semanticLayer.BasicDimensionSqlFn<
            {
              foo: string;
            },
            | "customer_id"
            | "first_name"
            | "created_at"
            | "created_at.date"
            | "created_at.time"
            | "created_at.hour"
            | "created_at.year"
            | "created_at.quarter"
            | "created_at.quarter_of_year"
            | "created_at.month"
            | "created_at.month_num"
            | "created_at.week"
            | "created_at.week_num"
            | "created_at.day_of_month"
            | "created_at.hour_of_day"
            | "created_at.minute"
            | "updated_at"
            | "updated_at.date"
            | "updated_at.time"
            | "updated_at.hour"
            | "updated_at.year"
            | "updated_at.quarter"
            | "updated_at.quarter_of_year"
            | "updated_at.month"
            | "updated_at.month_num"
            | "updated_at.week"
            | "updated_at.week_num"
            | "updated_at.day_of_month"
            | "updated_at.hour_of_day"
            | "updated_at.minute"
            | "private_dimension"
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
      private?: boolean | undefined;
      sql?:
        | semanticLayer.BasicDimensionSqlFn<
            {
              foo: string;
            },
            | "customer_id"
            | "first_name"
            | "created_at"
            | "created_at.date"
            | "created_at.time"
            | "created_at.hour"
            | "created_at.year"
            | "created_at.quarter"
            | "created_at.quarter_of_year"
            | "created_at.month"
            | "created_at.month_num"
            | "created_at.week"
            | "created_at.week_num"
            | "created_at.day_of_month"
            | "created_at.hour_of_day"
            | "created_at.minute"
            | "updated_at"
            | "updated_at.date"
            | "updated_at.time"
            | "updated_at.hour"
            | "updated_at.year"
            | "updated_at.quarter"
            | "updated_at.quarter_of_year"
            | "updated_at.month"
            | "updated_at.month_num"
            | "updated_at.week"
            | "updated_at.week_num"
            | "updated_at.day_of_month"
            | "updated_at.hour_of_day"
            | "updated_at.minute"
            | "private_dimension"
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
      private?: boolean | undefined;
      sql?:
        | semanticLayer.BasicDimensionSqlFn<
            {
              foo: string;
            },
            | "customer_id"
            | "first_name"
            | "created_at"
            | "created_at.date"
            | "created_at.time"
            | "created_at.hour"
            | "created_at.year"
            | "created_at.quarter"
            | "created_at.quarter_of_year"
            | "created_at.month"
            | "created_at.month_num"
            | "created_at.week"
            | "created_at.week_num"
            | "created_at.day_of_month"
            | "created_at.hour_of_day"
            | "created_at.minute"
            | "updated_at"
            | "updated_at.date"
            | "updated_at.time"
            | "updated_at.hour"
            | "updated_at.year"
            | "updated_at.quarter"
            | "updated_at.quarter_of_year"
            | "updated_at.month"
            | "updated_at.month_num"
            | "updated_at.week"
            | "updated_at.week_num"
            | "updated_at.day_of_month"
            | "updated_at.hour_of_day"
            | "updated_at.minute"
            | "private_dimension"
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
      private?: boolean | undefined;
      sql?:
        | semanticLayer.BasicDimensionSqlFn<
            {
              foo: string;
            },
            | "customer_id"
            | "first_name"
            | "created_at"
            | "created_at.date"
            | "created_at.time"
            | "created_at.hour"
            | "created_at.year"
            | "created_at.quarter"
            | "created_at.quarter_of_year"
            | "created_at.month"
            | "created_at.month_num"
            | "created_at.week"
            | "created_at.week_num"
            | "created_at.day_of_month"
            | "created_at.hour_of_day"
            | "created_at.minute"
            | "updated_at"
            | "updated_at.date"
            | "updated_at.time"
            | "updated_at.hour"
            | "updated_at.year"
            | "updated_at.quarter"
            | "updated_at.quarter_of_year"
            | "updated_at.month"
            | "updated_at.month_num"
            | "updated_at.week"
            | "updated_at.week_num"
            | "updated_at.day_of_month"
            | "updated_at.hour_of_day"
            | "updated_at.minute"
            | "private_dimension"
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
      private?: boolean | undefined;
      sql?:
        | semanticLayer.BasicDimensionSqlFn<
            {
              foo: string;
            },
            | "customer_id"
            | "first_name"
            | "created_at"
            | "created_at.date"
            | "created_at.time"
            | "created_at.hour"
            | "created_at.year"
            | "created_at.quarter"
            | "created_at.quarter_of_year"
            | "created_at.month"
            | "created_at.month_num"
            | "created_at.week"
            | "created_at.week_num"
            | "created_at.day_of_month"
            | "created_at.hour_of_day"
            | "created_at.minute"
            | "updated_at"
            | "updated_at.date"
            | "updated_at.time"
            | "updated_at.hour"
            | "updated_at.year"
            | "updated_at.quarter"
            | "updated_at.quarter_of_year"
            | "updated_at.month"
            | "updated_at.month_num"
            | "updated_at.week"
            | "updated_at.week_num"
            | "updated_at.day_of_month"
            | "updated_at.hour_of_day"
            | "updated_at.minute"
            | "private_dimension"
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
      private?: boolean | undefined;
      sql?:
        | semanticLayer.BasicDimensionSqlFn<
            {
              foo: string;
            },
            | "customer_id"
            | "first_name"
            | "created_at"
            | "created_at.date"
            | "created_at.time"
            | "created_at.hour"
            | "created_at.year"
            | "created_at.quarter"
            | "created_at.quarter_of_year"
            | "created_at.month"
            | "created_at.month_num"
            | "created_at.week"
            | "created_at.week_num"
            | "created_at.day_of_month"
            | "created_at.hour_of_day"
            | "created_at.minute"
            | "updated_at"
            | "updated_at.date"
            | "updated_at.time"
            | "updated_at.hour"
            | "updated_at.year"
            | "updated_at.quarter"
            | "updated_at.quarter_of_year"
            | "updated_at.month"
            | "updated_at.month_num"
            | "updated_at.week"
            | "updated_at.week_num"
            | "updated_at.day_of_month"
            | "updated_at.hour_of_day"
            | "updated_at.minute"
            | "private_dimension"
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
      private?: boolean | undefined;
      sql: semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        | "customer_id"
        | "first_name"
        | "created_at"
        | "created_at.date"
        | "created_at.time"
        | "created_at.hour"
        | "created_at.year"
        | "created_at.quarter"
        | "created_at.quarter_of_year"
        | "created_at.month"
        | "created_at.month_num"
        | "created_at.week"
        | "created_at.week_num"
        | "created_at.day_of_month"
        | "created_at.hour_of_day"
        | "created_at.minute"
        | "updated_at"
        | "updated_at.date"
        | "updated_at.time"
        | "updated_at.hour"
        | "updated_at.year"
        | "updated_at.quarter"
        | "updated_at.quarter_of_year"
        | "updated_at.month"
        | "updated_at.month_num"
        | "updated_at.week"
        | "updated_at.week_num"
        | "updated_at.day_of_month"
        | "updated_at.hour_of_day"
        | "updated_at.minute"
        | "private_dimension",
        "count" | "private_metric"
      >;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithMetric>[1], { type: "number" }>>
    >().toEqualTypeOf<{
      type: "number";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"number"> | undefined;
      private?: boolean | undefined;
      sql: semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        | "customer_id"
        | "first_name"
        | "created_at"
        | "created_at.date"
        | "created_at.time"
        | "created_at.hour"
        | "created_at.year"
        | "created_at.quarter"
        | "created_at.quarter_of_year"
        | "created_at.month"
        | "created_at.month_num"
        | "created_at.week"
        | "created_at.week_num"
        | "created_at.day_of_month"
        | "created_at.hour_of_day"
        | "created_at.minute"
        | "updated_at"
        | "updated_at.date"
        | "updated_at.time"
        | "updated_at.hour"
        | "updated_at.year"
        | "updated_at.quarter"
        | "updated_at.quarter_of_year"
        | "updated_at.month"
        | "updated_at.month_num"
        | "updated_at.week"
        | "updated_at.week_num"
        | "updated_at.day_of_month"
        | "updated_at.hour_of_day"
        | "updated_at.minute"
        | "private_dimension",
        "count" | "private_metric"
      >;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithMetric>[1], { type: "boolean" }>>
    >().toEqualTypeOf<{
      type: "boolean";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"boolean"> | undefined;
      private?: boolean | undefined;
      sql: semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        | "customer_id"
        | "first_name"
        | "created_at"
        | "created_at.date"
        | "created_at.time"
        | "created_at.hour"
        | "created_at.year"
        | "created_at.quarter"
        | "created_at.quarter_of_year"
        | "created_at.month"
        | "created_at.month_num"
        | "created_at.week"
        | "created_at.week_num"
        | "created_at.day_of_month"
        | "created_at.hour_of_day"
        | "created_at.minute"
        | "updated_at"
        | "updated_at.date"
        | "updated_at.time"
        | "updated_at.hour"
        | "updated_at.year"
        | "updated_at.quarter"
        | "updated_at.quarter_of_year"
        | "updated_at.month"
        | "updated_at.month_num"
        | "updated_at.week"
        | "updated_at.week_num"
        | "updated_at.day_of_month"
        | "updated_at.hour_of_day"
        | "updated_at.minute"
        | "private_dimension",
        "count" | "private_metric"
      >;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithMetric>[1], { type: "datetime" }>>
    >().toEqualTypeOf<{
      type: "datetime";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"datetime"> | undefined;
      private?: boolean | undefined;
      sql: semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        | "customer_id"
        | "first_name"
        | "created_at"
        | "created_at.date"
        | "created_at.time"
        | "created_at.hour"
        | "created_at.year"
        | "created_at.quarter"
        | "created_at.quarter_of_year"
        | "created_at.month"
        | "created_at.month_num"
        | "created_at.week"
        | "created_at.week_num"
        | "created_at.day_of_month"
        | "created_at.hour_of_day"
        | "created_at.minute"
        | "updated_at"
        | "updated_at.date"
        | "updated_at.time"
        | "updated_at.hour"
        | "updated_at.year"
        | "updated_at.quarter"
        | "updated_at.quarter_of_year"
        | "updated_at.month"
        | "updated_at.month_num"
        | "updated_at.week"
        | "updated_at.week_num"
        | "updated_at.day_of_month"
        | "updated_at.hour_of_day"
        | "updated_at.minute"
        | "private_dimension",
        "count" | "private_metric"
      >;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithMetric>[1], { type: "date" }>>
    >().toEqualTypeOf<{
      type: "date";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"date"> | undefined;
      private?: boolean | undefined;
      sql: semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        | "customer_id"
        | "first_name"
        | "created_at"
        | "created_at.date"
        | "created_at.time"
        | "created_at.hour"
        | "created_at.year"
        | "created_at.quarter"
        | "created_at.quarter_of_year"
        | "created_at.month"
        | "created_at.month_num"
        | "created_at.week"
        | "created_at.week_num"
        | "created_at.day_of_month"
        | "created_at.hour_of_day"
        | "created_at.minute"
        | "updated_at"
        | "updated_at.date"
        | "updated_at.time"
        | "updated_at.hour"
        | "updated_at.year"
        | "updated_at.quarter"
        | "updated_at.quarter_of_year"
        | "updated_at.month"
        | "updated_at.month_num"
        | "updated_at.week"
        | "updated_at.week_num"
        | "updated_at.day_of_month"
        | "updated_at.hour_of_day"
        | "updated_at.minute"
        | "private_dimension",
        "count" | "private_metric"
      >;
    }>();

    expectTypeOf<
      Simplify<Extract<Parameters<ModelWithMetric>[1], { type: "time" }>>
    >().toEqualTypeOf<{
      type: "time";
      description?: string | undefined;
      format?: semanticLayer.MemberFormat<"time"> | undefined;
      private?: boolean | undefined;
      sql: semanticLayer.BasicMetricSqlFn<
        {
          foo: string;
        },
        | "customer_id"
        | "first_name"
        | "created_at"
        | "created_at.date"
        | "created_at.time"
        | "created_at.hour"
        | "created_at.year"
        | "created_at.quarter"
        | "created_at.quarter_of_year"
        | "created_at.month"
        | "created_at.month_num"
        | "created_at.week"
        | "created_at.week_num"
        | "created_at.day_of_month"
        | "created_at.hour_of_day"
        | "created_at.minute"
        | "updated_at"
        | "updated_at.date"
        | "updated_at.time"
        | "updated_at.hour"
        | "updated_at.year"
        | "updated_at.quarter"
        | "updated_at.quarter_of_year"
        | "updated_at.month"
        | "updated_at.month_num"
        | "updated_at.week"
        | "updated_at.week_num"
        | "updated_at.day_of_month"
        | "updated_at.hour_of_day"
        | "updated_at.minute"
        | "private_dimension",
        "count" | "private_metric"
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
            created_at: "datetime";
            "created_at.date": "date";
            "created_at.time": "time";
            "created_at.hour": "string";
            "created_at.year": "number";
            "created_at.quarter": "string";
            "created_at.quarter_of_year": "number";
            "created_at.month": "string";
            "created_at.month_num": "number";
            "created_at.week": "string";
            "created_at.week_num": "number";
            "created_at.day_of_month": "number";
            "created_at.hour_of_day": "number";
            "created_at.minute": "string";
          }>;
          fromDimension<
            DN extends
              | "customer_id"
              | "first_name"
              | "created_at"
              | "created_at.date"
              | "created_at.time"
              | "created_at.hour"
              | "created_at.year"
              | "created_at.quarter"
              | "created_at.quarter_of_year"
              | "created_at.month"
              | "created_at.month_num"
              | "created_at.week"
              | "created_at.week_num"
              | "created_at.day_of_month"
              | "created_at.hour_of_day"
              | "created_at.minute",
          >(
            name: DN,
          ): semanticLayer.HierarchyElement<
            {
              customer_id: "number";
              first_name: "string";
              created_at: "datetime";
              "created_at.date": "date";
              "created_at.time": "time";
              "created_at.hour": "string";
              "created_at.year": "number";
              "created_at.quarter": "string";
              "created_at.quarter_of_year": "number";
              "created_at.month": "string";
              "created_at.month_num": "number";
              "created_at.week": "string";
              "created_at.week_num": "number";
              "created_at.day_of_month": "number";
              "created_at.hour_of_day": "number";
              "created_at.minute": "string";
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
          fromDimension<
            DN extends
              | "customer_id"
              | "first_name"
              | "created_at"
              | "created_at.date"
              | "created_at.time"
              | "created_at.hour"
              | "created_at.year"
              | "created_at.quarter"
              | "created_at.quarter_of_year"
              | "created_at.month"
              | "created_at.month_num"
              | "created_at.week"
              | "created_at.week_num"
              | "created_at.day_of_month"
              | "created_at.hour_of_day"
              | "created_at.minute",
          >(
            name: DN,
          ): semanticLayer.HierarchyElement<
            {
              customer_id: "number";
              first_name: "string";
              created_at: "datetime";
              "created_at.date": "date";
              "created_at.time": "time";
              "created_at.hour": "string";
              "created_at.year": "number";
              "created_at.quarter": "string";
              "created_at.quarter_of_year": "number";
              "created_at.month": "string";
              "created_at.month_num": "number";
              "created_at.week": "string";
              "created_at.week_num": "number";
              "created_at.day_of_month": "number";
              "created_at.hour_of_day": "number";
              "created_at.minute": "string";
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
      any,
      any
    >
      ? TMetrics
      : never;

    type GetRepositoryPrivateMembers<T> = T extends semanticLayer.Repository<
      any,
      any,
      any,
      any,
      infer TPrivateMembers,
      any,
      any
    >
      ? TPrivateMembers
      : never;

    type GetRepositoryHierarchies<T> = T extends semanticLayer.Repository<
      any,
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
      "customers.private_dimension": "string";
      "customers.created_at": "datetime";
      "customers.created_at.date": "date";
      "customers.created_at.time": "time";
      "customers.created_at.hour": "string";
      "customers.created_at.year": "number";
      "customers.created_at.quarter": "string";
      "customers.created_at.quarter_of_year": "number";
      "customers.created_at.month": "string";
      "customers.created_at.month_num": "number";
      "customers.created_at.week": "string";
      "customers.created_at.week_num": "number";
      "customers.created_at.day_of_month": "number";
      "customers.created_at.hour_of_day": "number";
      "customers.created_at.minute": "string";
      "customers.updated_at": "datetime";
      "customers.updated_at.date": "date";
      "customers.updated_at.time": "time";
      "customers.updated_at.hour": "string";
      "customers.updated_at.year": "number";
      "customers.updated_at.quarter": "string";
      "customers.updated_at.quarter_of_year": "number";
      "customers.updated_at.month": "string";
      "customers.updated_at.month_num": "number";
      "customers.updated_at.week": "string";
      "customers.updated_at.week_num": "number";
      "customers.updated_at.day_of_month": "number";
      "customers.updated_at.hour_of_day": "number";
      "customers.updated_at.minute": "string";
    }>();

    expectTypeOf<GetRepositoryMetrics<Repository>>().branded.toEqualTypeOf<{
      "customers.count": "number";
      "customers.private_metric": "number";
    }>();

    expectTypeOf<GetRepositoryPrivateMembers<Repository>>().toEqualTypeOf<
      | "customers.private_dimension"
      | "customers.private_metric"
      | "customers.updated_at"
      | "customers.updated_at.date"
      | "customers.updated_at.time"
      | "customers.updated_at.hour"
      | "customers.updated_at.year"
      | "customers.updated_at.quarter"
      | "customers.updated_at.quarter_of_year"
      | "customers.updated_at.month"
      | "customers.updated_at.month_num"
      | "customers.updated_at.week"
      | "customers.updated_at.week_num"
      | "customers.updated_at.day_of_month"
      | "customers.updated_at.hour_of_day"
      | "customers.updated_at.minute"
    >();

    expectTypeOf<GetRepositoryHierarchies<Repository>>().toEqualTypeOf<
      | "customers.customerHierarchy1"
      | "customers.customerHierarchy2"
      | "customers.created_at"
      | "repositoryHierarchy1"
      | "repositoryHierarchy2"
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
      private?: boolean | undefined;
      sql: semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.private_dimension"
        | "customers.created_at"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.updated_at"
        | "customers.updated_at.date"
        | "customers.updated_at.time"
        | "customers.updated_at.hour"
        | "customers.updated_at.year"
        | "customers.updated_at.quarter"
        | "customers.updated_at.quarter_of_year"
        | "customers.updated_at.month"
        | "customers.updated_at.month_num"
        | "customers.updated_at.week"
        | "customers.updated_at.week_num"
        | "customers.updated_at.day_of_month"
        | "customers.updated_at.hour_of_day"
        | "customers.updated_at.minute"
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
      private?: boolean | undefined;
      sql: semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.private_dimension"
        | "customers.created_at"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.updated_at"
        | "customers.updated_at.date"
        | "customers.updated_at.time"
        | "customers.updated_at.hour"
        | "customers.updated_at.year"
        | "customers.updated_at.quarter"
        | "customers.updated_at.quarter_of_year"
        | "customers.updated_at.month"
        | "customers.updated_at.month_num"
        | "customers.updated_at.week"
        | "customers.updated_at.week_num"
        | "customers.updated_at.day_of_month"
        | "customers.updated_at.hour_of_day"
        | "customers.updated_at.minute"
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
      private?: boolean | undefined;
      sql: semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.private_dimension"
        | "customers.created_at"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.updated_at"
        | "customers.updated_at.date"
        | "customers.updated_at.time"
        | "customers.updated_at.hour"
        | "customers.updated_at.year"
        | "customers.updated_at.quarter"
        | "customers.updated_at.quarter_of_year"
        | "customers.updated_at.month"
        | "customers.updated_at.month_num"
        | "customers.updated_at.week"
        | "customers.updated_at.week_num"
        | "customers.updated_at.day_of_month"
        | "customers.updated_at.hour_of_day"
        | "customers.updated_at.minute"
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
      private?: boolean | undefined;
      sql: semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.private_dimension"
        | "customers.created_at"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.updated_at"
        | "customers.updated_at.date"
        | "customers.updated_at.time"
        | "customers.updated_at.hour"
        | "customers.updated_at.year"
        | "customers.updated_at.quarter"
        | "customers.updated_at.quarter_of_year"
        | "customers.updated_at.month"
        | "customers.updated_at.month_num"
        | "customers.updated_at.week"
        | "customers.updated_at.week_num"
        | "customers.updated_at.day_of_month"
        | "customers.updated_at.hour_of_day"
        | "customers.updated_at.minute"
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
      private?: boolean | undefined;
      sql: semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.private_dimension"
        | "customers.created_at"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.updated_at"
        | "customers.updated_at.date"
        | "customers.updated_at.time"
        | "customers.updated_at.hour"
        | "customers.updated_at.year"
        | "customers.updated_at.quarter"
        | "customers.updated_at.quarter_of_year"
        | "customers.updated_at.month"
        | "customers.updated_at.month_num"
        | "customers.updated_at.week"
        | "customers.updated_at.week_num"
        | "customers.updated_at.day_of_month"
        | "customers.updated_at.hour_of_day"
        | "customers.updated_at.minute"
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
      private?: boolean | undefined;
      sql: semanticLayer.CalculatedDimensionSqlFn<
        {
          foo: string;
        },
        "customers",
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.private_dimension"
        | "customers.created_at"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.updated_at"
        | "customers.updated_at.date"
        | "customers.updated_at.time"
        | "customers.updated_at.hour"
        | "customers.updated_at.year"
        | "customers.updated_at.quarter"
        | "customers.updated_at.quarter_of_year"
        | "customers.updated_at.month"
        | "customers.updated_at.month_num"
        | "customers.updated_at.week"
        | "customers.updated_at.week_num"
        | "customers.updated_at.day_of_month"
        | "customers.updated_at.hour_of_day"
        | "customers.updated_at.minute"
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
      private?: boolean | undefined;
      sql: semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.private_dimension"
        | "customers.created_at"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.updated_at"
        | "customers.updated_at.date"
        | "customers.updated_at.time"
        | "customers.updated_at.hour"
        | "customers.updated_at.year"
        | "customers.updated_at.quarter"
        | "customers.updated_at.quarter_of_year"
        | "customers.updated_at.month"
        | "customers.updated_at.month_num"
        | "customers.updated_at.week"
        | "customers.updated_at.week_num"
        | "customers.updated_at.day_of_month"
        | "customers.updated_at.hour_of_day"
        | "customers.updated_at.minute",
        "customers.count" | "customers.private_metric"
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
      private?: boolean | undefined;
      sql: semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.private_dimension"
        | "customers.created_at"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.updated_at"
        | "customers.updated_at.date"
        | "customers.updated_at.time"
        | "customers.updated_at.hour"
        | "customers.updated_at.year"
        | "customers.updated_at.quarter"
        | "customers.updated_at.quarter_of_year"
        | "customers.updated_at.month"
        | "customers.updated_at.month_num"
        | "customers.updated_at.week"
        | "customers.updated_at.week_num"
        | "customers.updated_at.day_of_month"
        | "customers.updated_at.hour_of_day"
        | "customers.updated_at.minute",
        "customers.count" | "customers.private_metric"
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
      private?: boolean | undefined;
      sql: semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.private_dimension"
        | "customers.created_at"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.updated_at"
        | "customers.updated_at.date"
        | "customers.updated_at.time"
        | "customers.updated_at.hour"
        | "customers.updated_at.year"
        | "customers.updated_at.quarter"
        | "customers.updated_at.quarter_of_year"
        | "customers.updated_at.month"
        | "customers.updated_at.month_num"
        | "customers.updated_at.week"
        | "customers.updated_at.week_num"
        | "customers.updated_at.day_of_month"
        | "customers.updated_at.hour_of_day"
        | "customers.updated_at.minute",
        "customers.count" | "customers.private_metric"
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
      private?: boolean | undefined;
      sql: semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.private_dimension"
        | "customers.created_at"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.updated_at"
        | "customers.updated_at.date"
        | "customers.updated_at.time"
        | "customers.updated_at.hour"
        | "customers.updated_at.year"
        | "customers.updated_at.quarter"
        | "customers.updated_at.quarter_of_year"
        | "customers.updated_at.month"
        | "customers.updated_at.month_num"
        | "customers.updated_at.week"
        | "customers.updated_at.week_num"
        | "customers.updated_at.day_of_month"
        | "customers.updated_at.hour_of_day"
        | "customers.updated_at.minute",
        "customers.count" | "customers.private_metric"
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
      private?: boolean | undefined;
      sql: semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.private_dimension"
        | "customers.created_at"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.updated_at"
        | "customers.updated_at.date"
        | "customers.updated_at.time"
        | "customers.updated_at.hour"
        | "customers.updated_at.year"
        | "customers.updated_at.quarter"
        | "customers.updated_at.quarter_of_year"
        | "customers.updated_at.month"
        | "customers.updated_at.month_num"
        | "customers.updated_at.week"
        | "customers.updated_at.week_num"
        | "customers.updated_at.day_of_month"
        | "customers.updated_at.hour_of_day"
        | "customers.updated_at.minute",
        "customers.count" | "customers.private_metric"
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
      private?: boolean | undefined;
      sql: semanticLayer.CalculatedMetricSqlFn<
        {
          foo: string;
        },
        "customers",
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.private_dimension"
        | "customers.created_at"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.updated_at"
        | "customers.updated_at.date"
        | "customers.updated_at.time"
        | "customers.updated_at.hour"
        | "customers.updated_at.year"
        | "customers.updated_at.quarter"
        | "customers.updated_at.quarter_of_year"
        | "customers.updated_at.month"
        | "customers.updated_at.month_num"
        | "customers.updated_at.week"
        | "customers.updated_at.week_num"
        | "customers.updated_at.day_of_month"
        | "customers.updated_at.hour_of_day"
        | "customers.updated_at.minute",
        "customers.count" | "customers.private_metric"
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
            "customers.created_at": "datetime";
            "customers.created_at.date": "date";
            "customers.created_at.time": "time";
            "customers.created_at.hour": "string";
            "customers.created_at.year": "number";
            "customers.created_at.quarter": "string";
            "customers.created_at.quarter_of_year": "number";
            "customers.created_at.month": "string";
            "customers.created_at.month_num": "number";
            "customers.created_at.week": "string";
            "customers.created_at.week_num": "number";
            "customers.created_at.day_of_month": "number";
            "customers.created_at.hour_of_day": "number";
            "customers.created_at.minute": "string";
          }>;
          fromDimension<
            DN extends
              | "customers.customer_id"
              | "customers.first_name"
              | "customers.created_at"
              | "customers.created_at.date"
              | "customers.created_at.time"
              | "customers.created_at.hour"
              | "customers.created_at.year"
              | "customers.created_at.quarter"
              | "customers.created_at.quarter_of_year"
              | "customers.created_at.month"
              | "customers.created_at.month_num"
              | "customers.created_at.week"
              | "customers.created_at.week_num"
              | "customers.created_at.day_of_month"
              | "customers.created_at.hour_of_day"
              | "customers.created_at.minute",
          >(
            name: DN,
          ): semanticLayer.HierarchyElement<
            {
              "customers.customer_id": "number";
              "customers.first_name": "string";
              "customers.created_at": "datetime";
              "customers.created_at.date": "date";
              "customers.created_at.time": "time";
              "customers.created_at.hour": "string";
              "customers.created_at.year": "number";
              "customers.created_at.quarter": "string";
              "customers.created_at.quarter_of_year": "number";
              "customers.created_at.month": "string";
              "customers.created_at.month_num": "number";
              "customers.created_at.week": "string";
              "customers.created_at.week_num": "number";
              "customers.created_at.day_of_month": "number";
              "customers.created_at.hour_of_day": "number";
              "customers.created_at.minute": "string";
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
            "customers.created_at": "datetime";
            "customers.created_at.date": "date";
            "customers.created_at.time": "time";
            "customers.created_at.hour": "string";
            "customers.created_at.year": "number";
            "customers.created_at.quarter": "string";
            "customers.created_at.quarter_of_year": "number";
            "customers.created_at.month": "string";
            "customers.created_at.month_num": "number";
            "customers.created_at.week": "string";
            "customers.created_at.week_num": "number";
            "customers.created_at.day_of_month": "number";
            "customers.created_at.hour_of_day": "number";
            "customers.created_at.minute": "string";
          }>;
          fromDimension<
            DN extends
              | "customers.customer_id"
              | "customers.first_name"
              | "customers.created_at"
              | "customers.created_at.date"
              | "customers.created_at.time"
              | "customers.created_at.hour"
              | "customers.created_at.year"
              | "customers.created_at.quarter"
              | "customers.created_at.quarter_of_year"
              | "customers.created_at.month"
              | "customers.created_at.month_num"
              | "customers.created_at.week"
              | "customers.created_at.week_num"
              | "customers.created_at.day_of_month"
              | "customers.created_at.hour_of_day"
              | "customers.created_at.minute",
          >(
            name: DN,
          ): semanticLayer.HierarchyElement<
            {
              "customers.customer_id": "number";
              "customers.first_name": "string";
              "customers.created_at": "datetime";
              "customers.created_at.date": "date";
              "customers.created_at.time": "time";
              "customers.created_at.hour": "string";
              "customers.created_at.year": "number";
              "customers.created_at.quarter": "string";
              "customers.created_at.quarter_of_year": "number";
              "customers.created_at.month": "string";
              "customers.created_at.month_num": "number";
              "customers.created_at.week": "string";
              "customers.created_at.week_num": "number";
              "customers.created_at.day_of_month": "number";
              "customers.created_at.hour_of_day": "number";
              "customers.created_at.minute": "string";
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
      | "customers.created_at"
      | "customers.customer_id"
      | "customers.first_name"
      | "customers.created_at.date"
      | "customers.created_at.time"
      | "customers.created_at.hour"
      | "customers.created_at.year"
      | "customers.created_at.quarter"
      | "customers.created_at.quarter_of_year"
      | "customers.created_at.month"
      | "customers.created_at.month_num"
      | "customers.created_at.week"
      | "customers.created_at.week_num"
      | "customers.created_at.day_of_month"
      | "customers.created_at.hour_of_day"
      | "customers.created_at.minute"
      | "customers.count"
    >();

    expectTypeOf<NonNullable<Q["order"]>[number]>().toEqualTypeOf<{
      member:
        | "customers.created_at"
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
        | "customers.count";
      direction: "asc" | "desc";
    }>();

    expectTypeOf<Extract<QF, { operator: "equals" }>>().toEqualTypeOf<{
      operator: "equals";
      member:
        | "customers.created_at"
        | "customers.customer_id"
        | "customers.first_name"
        | "customers.created_at.date"
        | "customers.created_at.time"
        | "customers.created_at.hour"
        | "customers.created_at.year"
        | "customers.created_at.quarter"
        | "customers.created_at.quarter_of_year"
        | "customers.created_at.month"
        | "customers.created_at.month_num"
        | "customers.created_at.week"
        | "customers.created_at.week_num"
        | "customers.created_at.day_of_month"
        | "customers.created_at.hour_of_day"
        | "customers.created_at.minute"
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
