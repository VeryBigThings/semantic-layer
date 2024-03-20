# @verybigthings/semantic-layer

![NPM](https://img.shields.io/npm/l/@verybigthings/semantic-layer)
![NPM](https://img.shields.io/npm/v/@verybigthings/semantic-layer)
![GitHub Workflow Status](https://github.com/verybigthings/semantic-layer/actions/workflows/semantic-layer.yml/badge.svg?branch=main)

## Introduction

The `@verybigthings/semantic-layer` library is crafted to simplify interactions between applications and relational databases, by providing a framework that abstracts SQL query complexities into a more manageable form. It aids in constructing analytical queries while addressing common issues such as join fanout and chasm traps. The library intelligently determines optimal join strategies for requested models, based on their definitions within the database. Designed for direct integration into existing code bases, it operates without the need for deploying external services.

## Key Features

- **Declarative Schema and Query Building:** Utilize a fluent, TypeScript-based API to define your database schema and queries declaratively.
- **Type Safety:** Minimize errors with type-safe interfaces for query construction, enhancing code reliability.
- **Dynamic SQL Query Generation:** Automatically construct complex SQL queries tailored to your application's business logic, eliminating the need for string concatenation.

## Getting Started

### Installation

To integrate the Semantic Layer Library into your project, run the following command with npm:

```shell
npm install @verybigthings/semantic-layer
```

## Usage Examples

### Defining Models and Fields

This library allows you to define models and their respective fields, including dimensions and metrics, which represent the various columns and computed values within your database.

**Defining a Model:**

```typescript
import * as semanticLayer from "@verybigthings/semantic-layer";

const customersModel = semanticLayer
  .model("customers")
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
  .withDimension("last_name", {
    type: "string",
    sql: ({ model }) => model.column("LastName"),
  });

const invoicesModel = semanticLayer
  .model("invoices")
  .fromTable("Invoice")
  .withDimension("invoice_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model, sql }) => sql`${model.column("InvoiceId")}`,
  })
  .withMetric("total", {
    // node-postgres returns string types for big integers
    type: "string",
    aggregateWith: "sum",
    sql: ({ model }) => model.column("Total"),
  });

const invoiceLinesModel = semanticLayer
  .model("invoice_lines")
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
  })
  .withMetric("quantity", {
    // node-postgres returns string types for big integers
    type: "string",
    aggregateWith: "sum",
    sql: ({ model }) => model.column("Quantity"),
  })
  .withMetric("total_unit_price", {
    // node-postgres returns string types for big integers

    type: "string",
    aggregateWith: "sum"
    sql: ({ model }) => model.column("UnitPrice"),
  });
```

**Defining a Repository and joining models:**

```typescript
const repository = semanticLayer
  .repository()
  .withModel(customersModel)
  .withModel(invoicesModel)
  .withModel(invoiceLinesModel)
  .joinOneToMany(
    "customers",
    "invoices",
    ({ sql, dimensions }) =>
      sql`${dimensions.customers.customer_id} = ${dimensions.invoices.customer_id}`
  )
  .joinOneToMany(
    "invoices",
    "invoice_lines",
    ({ sql, dimensions }) =>
      sql`${dimensions.invoices.invoice_id} = ${dimensions.invoice_lines.invoice_id}`
  );

const queryBuilder = repository.build("postgresql");
```

### Data Querying

Leverage the library's querying capabilities to fetch dimensions and metrics, apply filters, and sort results efficiently.

```typescript
// Dimension and metric query
const query = queryBuilder.buildQuery({
  dimensions: ["customers.customer_id"],
  metrics: ["invoices.total"],
  order: { "customers.customer_id": "asc" },
  limit: 10,
});

// Metric query with filters
const query = queryBuilder.buildQuery({
  metrics: ["invoices.total", "invoice_lines.quantity"],
  filters: [
    { operator: "equals", member: "customers.customer_id", value: [1] },
  ],
});

// Dimension query with filters
const query = queryBuilder.buildQuery({
  dimensions: ["customers.first_name", "customers.last_name"],
  filters: [
    { operator: "equals", member: "customers.customer_id", value: [1] },
  ],
});

// Filtering and sorting
const query = queryBuilder.buildQuery({
  dimensions: ["customers.first_name"],
  metrics: ["invoices.total"],
  filters: [{ operator: "gt", member: "invoices.total", value: [100] }],
  order: { "invoices.total": "desc" },
});
```

### Executing queries

Note: `@verybigthings/semantic-layer` focuses on SQL generation. Execute the generated queries with your SQL client:

```typescript
const result = await sqlClient.query(query.sql, query.bindings);
```

### Limitations

At the moment, only PostgreSQL queries are generated correctly. We're working on adding support for additional dialects.

## Acknowledgments

`@verybigthings/semantic-layer` draws inspiration from several BI libraries, particularly [Cube.dev](https://cube.dev). While our API is very close to that of Cube.dev, future development may change our approach.
