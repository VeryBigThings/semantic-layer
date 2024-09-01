import * as semanticLayer from "../index.js";

import { assert, it } from "vitest";

const userModel = semanticLayer
  .model()
  .withName("user")
  .fromTable("User")
  .withDimension("user_id", {
    type: "number",
    primaryKey: true,
  })
  .withDimension("first_name", {
    type: "string",
  })
  .withDimension("last_name", {
    type: "string",
  })
  .withDimension("datetime", {
    type: "datetime",
    format: (value) => `DateTime: ${value ? value.getTime() / 1000 : "-"}`,
  })
  .withMetric("count", {
    type: "string",
    sql: ({ model, sql }) => sql`COUNT(DISTINCT ${model.column("CustomerId")})`,
    format: (value) => `Count: ${value}`,
  })
  .withMetric("percentage", {
    type: "string",
    sql: ({ model, sql }) => sql`COUNT(DISTINCT ${model.column("CustomerId")})`,
    format: "percentage",
  })
  .withMetric("currency", {
    type: "string",
    sql: ({ model, sql }) => sql`COUNT(DISTINCT ${model.column("CustomerId")})`,
    format: "currency",
  });

const repository = semanticLayer.repository().withModel(userModel);
const queryBuilder = repository.build("postgresql");
it("can format results", () => {
  const query: semanticLayer.AnyInputQuery = {
    members: [
      "user.user_id",
      "user.first_name",
      "user.last_name",
      "user.datetime",
      "user.percentage",
      "user.currency",
      "user.count",
    ],
  };

  const introspection = queryBuilder.introspect(query);
  const now = new Date();
  const results = [
    {
      user___user_id: 1,
      user___first_name: "First",
      user___last_name: "Last",
      user___datetime: now,
      user___count: 10,
      user___percentage: 10,
      user___currency: 10,
    },
  ];

  const formattedResults = semanticLayer.helpers.formatResults(
    results,
    introspection,
  );
  assert.deepEqual(formattedResults, [
    {
      user___user_id: {
        value: 1,
      },
      user___first_name: {
        value: "First",
      },
      user___last_name: {
        value: "Last",
      },
      user___datetime: {
        value: now,
        formattedValue: `DateTime: ${now.getTime() / 1000}`,
      },
      user___count: {
        value: 10,
        formattedValue: "Count: 10",
      },
      user___percentage: {
        value: 10,
        formattedValue: "10%",
      },
      user___currency: {
        value: 10,
        formattedValue: "$10",
      },
    },
  ]);
});

it("can convert path to alias", () => {
  assert.equal(
    semanticLayer.helpers.pathToAlias("user.user_id"),
    "user___user_id",
  );
});

it("can convert alias to path", () => {
  assert.equal(
    semanticLayer.helpers.aliasToPath("user___user_id"),
    "user.user_id",
  );
});
