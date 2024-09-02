import * as semanticLayer from "../index.js";

import { assert, describe, it } from "vitest";

const aModel = semanticLayer
  .model()
  .withName("a")
  .fromTable("a")
  .withDimension("a_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model, sql }) => sql`${model.column("a_id")}`,
  });

const bModel = semanticLayer
  .model()
  .withName("b")
  .fromTable("b")
  .withDimension("b_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model, sql }) => sql`${model.column("b_id")}`,
  })
  .withDimension("a_id", {
    type: "number",
    sql: ({ model }) => model.column("a_id"),
  });

const cModel = semanticLayer
  .model()
  .withName("c")
  .fromTable("c")
  .withDimension("c_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model, sql }) => sql`${model.column("c_id")}`,
  })
  .withDimension("a_id", {
    type: "number",
    sql: ({ model }) => model.column("a_id"),
  });

const dModel = semanticLayer
  .model()
  .withName("d")
  .fromTable("d")
  .withDimension("d_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model, sql }) => sql`${model.column("d_id")}`,
  })
  .withDimension("b_id", {
    type: "number",
    sql: ({ model }) => model.column("b_id"),
  })
  .withDimension("c_id", {
    type: "number",
    sql: ({ model }) => model.column("c_id"),
  });

describe("join plan", () => {
  it("can generate a join plan when no joins have priority set", () => {
    const repository = semanticLayer
      .repository()
      .withModel(aModel)
      .withModel(bModel)
      .withModel(cModel)
      .withModel(dModel)
      .joinOneToOne(
        "a",
        "b",
        ({ sql, models }) =>
          sql`${models.a.dimension("a_id")} = ${models.b.dimension("a_id")}`,
      )
      .joinOneToMany(
        "a",
        "c",
        ({ sql, models }) =>
          sql`${models.a.dimension("a_id")} = ${models.c.dimension("a_id")}`,
      )
      .joinOneToOne(
        "b",
        "d",
        ({ sql, models }) =>
          sql`${models.b.dimension("b_id")} = ${models.d.dimension("b_id")}`,
      )
      .joinOneToMany(
        "c",
        "d",
        ({ sql, models }) =>
          sql`${models.c.dimension("c_id")} = ${models.d.dimension("c_id")}`,
      );

    const queryBuilder = repository.build("postgresql");

    const queryContext = new semanticLayer.QueryContext(
      queryBuilder.repository,
      queryBuilder.dialect,
      undefined,
    );
    const query: semanticLayer.AnyInputQuery = {
      members: ["a.a_id", "b.b_id", "c.c_id", "d.d_id"],
    };
    const queryPlan = queryBuilder.getQueryPlan(queryContext, undefined, query);
    assert.deepEqual(queryPlan.segments[0]!.joinPlan, {
      hasRowMultiplication: true,
      initialModel: "a",
      joins: [
        {
          leftModel: "a",
          rightModel: "c",
          joinType: "left",
        },
        {
          leftModel: "a",
          rightModel: "b",
          joinType: "left",
        },
        {
          leftModel: "b",
          rightModel: "d",
          joinType: "left",
        },
      ],
    });
  });

  it("can generate a join plan when joins have priority set", () => {
    const repository = semanticLayer
      .repository()
      .withModel(aModel)
      .withModel(bModel)
      .withModel(cModel)
      .withModel(dModel)
      .joinOneToOne(
        "a",
        "b",
        ({ sql, models }) =>
          sql`${models.a.dimension("a_id")} = ${models.b.dimension("a_id")}`,
      )
      .joinOneToMany(
        "a",
        "c",
        ({ sql, models }) =>
          sql`${models.a.dimension("a_id")} = ${models.c.dimension("a_id")}`,
      )
      .joinOneToOne(
        "b",
        "d",
        ({ sql, models }) =>
          sql`${models.b.dimension("b_id")} = ${models.d.dimension("b_id")}`,
      )
      .joinOneToMany(
        "c",
        "d",
        ({ sql, models }) =>
          sql`${models.c.dimension("c_id")} = ${models.d.dimension("c_id")}`,
        { priority: "high" },
      );

    const queryBuilder = repository.build("postgresql");

    const queryContext = new semanticLayer.QueryContext(
      queryBuilder.repository,
      queryBuilder.dialect,
      undefined,
    );
    const query1: semanticLayer.AnyInputQuery = {
      members: ["a.a_id", "b.b_id", "c.c_id", "d.d_id"],
    };
    const queryPlan1 = queryBuilder.getQueryPlan(
      queryContext,
      undefined,
      query1,
    );

    assert.deepEqual(queryPlan1.segments[0]!.joinPlan, {
      hasRowMultiplication: false,
      initialModel: "a",
      joins: [
        {
          leftModel: "a",
          rightModel: "b",
          joinType: "left",
        },
        {
          leftModel: "b",
          rightModel: "d",
          joinType: "left",
        },
        {
          leftModel: "d",
          rightModel: "c",
          joinType: "right",
        },
      ],
    });
    const query2: semanticLayer.AnyInputQuery = {
      members: ["a.a_id", "b.b_id", "d.d_id"],
    };
    const queryPlan2 = queryBuilder.getQueryPlan(
      queryContext,
      undefined,
      query2,
    );

    assert.deepEqual(queryPlan2.segments[0]!.joinPlan, {
      hasRowMultiplication: false,
      initialModel: "a",
      joins: [
        { leftModel: "a", rightModel: "b", joinType: "left" },
        { leftModel: "b", rightModel: "d", joinType: "left" },
      ],
    });
  });

  it("can generate a join plan with explicit join type set", () => {
    const repository = semanticLayer
      .repository()
      .withModel(aModel)
      .withModel(bModel)
      .withModel(cModel)
      .withModel(dModel)
      .joinOneToOne(
        "a",
        "b",
        ({ sql, models }) =>
          sql`${models.a.dimension("a_id")} = ${models.b.dimension("a_id")}`,
        { type: "inner" },
      )
      .joinOneToMany(
        "a",
        "c",
        ({ sql, models }) =>
          sql`${models.a.dimension("a_id")} = ${models.c.dimension("a_id")}`,
      )
      .joinOneToOne(
        "b",
        "d",
        ({ sql, models }) =>
          sql`${models.b.dimension("b_id")} = ${models.d.dimension("b_id")}`,
        { type: "full" },
      )
      .joinOneToMany(
        "c",
        "d",
        ({ sql, models }) =>
          sql`${models.c.dimension("c_id")} = ${models.d.dimension("c_id")}`,
        { priority: "high" },
      );

    const queryBuilder = repository.build("postgresql");

    const queryContext = new semanticLayer.QueryContext(
      queryBuilder.repository,
      queryBuilder.dialect,
      undefined,
    );
    const query: semanticLayer.AnyInputQuery = {
      members: ["a.a_id", "b.b_id", "c.c_id", "d.d_id"],
    };
    const queryPlan = queryBuilder.getQueryPlan(queryContext, undefined, query);

    assert.deepEqual(queryPlan.segments[0]!.joinPlan, {
      hasRowMultiplication: false,
      initialModel: "a",
      joins: [
        {
          leftModel: "a",
          rightModel: "b",
          joinType: "inner",
        },
        {
          leftModel: "b",
          rightModel: "d",
          joinType: "full",
        },
        {
          leftModel: "d",
          rightModel: "c",
          joinType: "right",
        },
      ],
    });

    const { sql } = queryBuilder.unsafeBuildQuery(query, undefined);

    assert.equal(
      sql,
      'select "q0"."a___a_id" as "a___a_id", "q0"."b___b_id" as "b___b_id", "q0"."c___c_id" as "c___c_id", "q0"."d___d_id" as "d___d_id" from (select "a"."a_id" as "a___a_id", "b"."b_id" as "b___b_id", "c"."c_id" as "c___c_id", "d"."d_id" as "d___d_id" from "a" inner join "b" on "a"."a_id" = "b"."a_id" full join "d" on "b"."b_id" = "d"."b_id" right join "c" on "c"."c_id" = "d"."c_id") as "q0" group by "q0"."a___a_id", "q0"."b___b_id", "q0"."c___c_id", "q0"."d___d_id" order by "a___a_id" asc',
    );
  });
});
