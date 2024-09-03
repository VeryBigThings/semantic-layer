import * as semanticLayer from "../index.js";

import { assert, describe, it } from "vitest";

const userModel = semanticLayer
  .model()
  .withName("user")
  .fromTable("User")
  .withDimension("user_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model, sql }) => sql`${model.column("UserId")}`,
  })
  .withDimension("first_name", {
    type: "string",
    sql: ({ model }) => model.column("FirstName"),
  })
  .withDimension("last_name", {
    type: "string",
    sql: ({ model }) => model.column("LastName"),
  })
  .withDimension("datetime", {
    type: "datetime",
    sql: ({ model }) => model.column("DateTime"),
  })
  .withMetric("count", {
    type: "string",
    sql: ({ model, sql }) => sql`COUNT(DISTINCT ${model.column("UserId")})`,
  })
  .withCategoricalHierarchy("customer", ({ element }) => [
    element("user")
      .withDimensions(["user_id", "first_name", "last_name"])
      .withFormat(
        ["first_name", "last_name"],
        ({ dimension }) =>
          `${dimension("first_name")} ${dimension("last_name")}`,
      ),
  ]);

const customerModel = userModel.clone("customer");
const employeeModel = userModel.clone("employee");

const invoiceModel = semanticLayer
  .model()
  .withName("invoice")
  .fromTable("Invoice")
  .withDimension("invoice_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model }) => model.column("InvoiceId"),
  })
  .withDimension("customer_id", {
    type: "number",
    sql: ({ model }) => model.column("CustomerId"),
  })
  .withDimension("employee_id", {
    type: "number",
    sql: ({ model }) => model.column("EmployeeId"),
  });

const repository = semanticLayer
  .repository()
  .withModel(customerModel)
  .withModel(employeeModel)
  .withModel(invoiceModel)
  .joinOneToMany(
    "customer",
    "invoice",
    ({ sql, models }) =>
      sql`${models.customer.dimension("user_id")} = ${models.invoice.dimension(
        "customer_id",
      )}`,
  )
  .joinOneToMany(
    "employee",
    "invoice",
    ({ sql, models }) =>
      sql`${models.employee.dimension("user_id")} = ${models.invoice.dimension(
        "employee_id",
      )}`,
  );

const queryBuilder = repository.build("postgresql");

describe("clone", () => {
  it("can clone a model", () => {
    assert.deepEqual(
      userModel.categoricalHierarchies,
      customerModel.categoricalHierarchies,
    );
    assert.deepEqual(userModel.hierarchyNames, customerModel.hierarchyNames);

    const query = queryBuilder.buildQuery({
      members: [
        "customer.user_id",
        "customer.count",
        "employee.user_id",
        "employee.count",
        "invoice.invoice_id",
      ],
    });

    assert.equal(
      query.sql,
      'select "q0"."customer___user_id" as "customer___user_id", "q0"."employee___user_id" as "employee___user_id", "q0"."invoice___invoice_id" as "invoice___invoice_id", "q0"."customer___count" as "customer___count", "q1"."employee___count" as "employee___count" from (select "s0"."customer___user_id" as "customer___user_id", "s0"."employee___user_id" as "employee___user_id", "s0"."invoice___invoice_id" as "invoice___invoice_id", COUNT(DISTINCT "customer___count___mr_0") as "customer___count" from (select distinct "User"."UserId" as "customer___user_id", "User"."UserId" as "employee___user_id", "Invoice"."InvoiceId" as "invoice___invoice_id", "User"."UserId" as "customer___count___mr_0" from "User" left join "Invoice" on "User"."UserId" = "Invoice"."CustomerId" right join "User" on "User"."UserId" = "Invoice"."EmployeeId") as "s0" group by "s0"."customer___user_id", "s0"."employee___user_id", "s0"."invoice___invoice_id") as "q0" inner join (select "s1"."customer___user_id" as "customer___user_id", "s1"."employee___user_id" as "employee___user_id", "s1"."invoice___invoice_id" as "invoice___invoice_id", COUNT(DISTINCT "employee___count___mr_0") as "employee___count" from (select distinct "User"."UserId" as "customer___user_id", "User"."UserId" as "employee___user_id", "Invoice"."InvoiceId" as "invoice___invoice_id", "User"."UserId" as "employee___count___mr_0" from "User" left join "Invoice" on "User"."UserId" = "Invoice"."EmployeeId" right join "User" on "User"."UserId" = "Invoice"."CustomerId") as "s1" group by "s1"."customer___user_id", "s1"."employee___user_id", "s1"."invoice___invoice_id") as "q1" on "q0"."customer___user_id" = "q1"."customer___user_id" and "q0"."employee___user_id" = "q1"."employee___user_id" and "q0"."invoice___invoice_id" = "q1"."invoice___invoice_id" order by "customer___count" desc',
    );
  });
});
