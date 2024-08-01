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
  .withMetric("count", {
    type: "string",
    sql: ({ model, sql }) => sql`COUNT(DISTINCT ${model.column("UserId")})`,
  });

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

describe("clone", async () => {
  it("can clone a model", async () => {
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
      'select "q0"."customer___user_id" as "customer___user_id", "q0"."employee___user_id" as "employee___user_id", "q0"."invoice___invoice_id" as "invoice___invoice_id", "q0"."customer___count" as "customer___count", "q1"."employee___count" as "employee___count" from (select "customer_query"."customer___user_id" as "customer___user_id", "customer_query"."employee___user_id" as "employee___user_id", "customer_query"."invoice___invoice_id" as "invoice___invoice_id", COUNT(DISTINCT "count___metric_ref_0") as "customer___count" from (select distinct "User"."UserId" as "count___metric_ref_0", "User"."UserId" as "customer___user_id", "Invoice"."InvoiceId" as "invoice___invoice_id", "User"."UserId" as "employee___user_id" from "User" left join "Invoice" on "User"."UserId" = "Invoice"."CustomerId" right join "User" on "User"."UserId" = "Invoice"."EmployeeId") as "customer_query" group by "customer_query"."customer___user_id", "customer_query"."employee___user_id", "customer_query"."invoice___invoice_id") as "q0" inner join (select "employee_query"."customer___user_id" as "customer___user_id", "employee_query"."employee___user_id" as "employee___user_id", "employee_query"."invoice___invoice_id" as "invoice___invoice_id", COUNT(DISTINCT "count___metric_ref_0") as "employee___count" from (select distinct "User"."UserId" as "count___metric_ref_0", "User"."UserId" as "employee___user_id", "Invoice"."InvoiceId" as "invoice___invoice_id", "User"."UserId" as "customer___user_id" from "User" left join "Invoice" on "User"."UserId" = "Invoice"."EmployeeId" right join "User" on "User"."UserId" = "Invoice"."CustomerId") as "employee_query" group by "employee_query"."customer___user_id", "employee_query"."employee___user_id", "employee_query"."invoice___invoice_id") as "q1" on "q0"."customer___user_id" = "q1"."customer___user_id" and "q0"."employee___user_id" = "q1"."employee___user_id" and "q0"."invoice___invoice_id" = "q1"."invoice___invoice_id" order by "customer___count" desc limit $1 offset $2',
    );
  });
});
