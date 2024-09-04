import * as semanticLayer from "../index.js";

import { assert, it } from "vitest";

const customersModel = semanticLayer
  .model()
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
  });

const invoicesModel = semanticLayer
  .model()
  .withName("invoices")
  .fromTable("Invoice")
  .withDimension("invoice_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model }) => model.column("InvoiceId"),
  })
  .withDimension("customer_id", {
    type: "number",
    sql: ({ model }) => model.column("CustomerId"),
  });

const invoiceLinesModel = semanticLayer
  .model()
  .withName("invoice_lines")
  .fromTable("InvoiceLine")
  .withDimension("invoice_line_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model }) => model.column("InvoiceLineId"),
  })
  .withDimension("invoice_id", {
    type: "number",
    sql: ({ model }) => model.column("InvoiceId"),
  })
  .withDimension("track_id", {
    type: "number",
    sql: ({ model }) => model.column("TrackId"),
  });

const tracksModel = semanticLayer
  .model()
  .withName("tracks")
  .fromTable("Track")
  .withDimension("track_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model }) => model.column("TrackId"),
  });

it("will correctly check if all models are connected when no joins exists", () => {
  const repository = semanticLayer
    .repository()
    .withModel(customersModel)
    .withModel(invoicesModel)
    .withModel(invoiceLinesModel)
    .withModel(tracksModel);

  assert.throws(() => {
    repository.build("postgresql");
  }, "All models in a repository must be connected.");
});

it("will correctly check if all models are connected when only some models are connected (1)", () => {
  const repository = semanticLayer
    .repository()
    .withModel(customersModel)
    .withModel(invoicesModel)
    .withModel(invoiceLinesModel)
    .withModel(tracksModel)
    .joinOneToMany(
      "customers",
      "invoices",
      ({ sql, models }) =>
        sql`${models.customers.dimension(
          "customer_id",
        )} = ${models.invoices.dimension("customer_id")}`,
    );

  assert.throws(() => {
    repository.build("postgresql");
  }, "All models in a repository must be connected.");
});

it("will correctly check if all models are connected when only some models are connected (2)", () => {
  const repository = semanticLayer
    .repository()
    .withModel(customersModel)
    .withModel(invoicesModel)
    .withModel(invoiceLinesModel)
    .withModel(tracksModel)
    .joinOneToMany(
      "customers",
      "invoices",
      ({ sql, models }) =>
        sql`${models.customers.dimension(
          "customer_id",
        )} = ${models.invoices.dimension("customer_id")}`,
    )
    .joinManyToOne(
      "invoice_lines",
      "tracks",
      ({ sql, models }) =>
        sql`${models.invoice_lines.dimension(
          "track_id",
        )} = ${models.tracks.dimension("track_id")}`,
    );

  assert.throws(() => {
    repository.build("postgresql");
  }, "All models in a repository must be connected.");
});
