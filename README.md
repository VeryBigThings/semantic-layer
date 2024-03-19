# @verybigthings/semantic-layer

![NPM](https://img.shields.io/npm/l/@verybigthings/semantic-layer)
![NPM](https://img.shields.io/npm/v/@verybigthings/semantic-layer)
![GitHub Workflow Status](https://github.com/verybigthings/semantic-layer/actions/workflows/semantic-layer.yml/badge.svg?branch=main)

## Introduction

The `@verybigthings/semantic-layer` library is crafted to simplify interactions between applications and relational databases, by providing a framework that abstracts SQL query complexities into a more manageable form. It aids in constructing analytical queries while addressing common issues such as join fanout and chasm traps. The library intelligently determines optimal join strategies for requested tables, based on their definitions within the database. Designed for direct integration into existing code bases, it operates without the need for deploying external services.

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

### Defining Tables and Fields

This library allows you to define tables and their respective fields, including dimensions and metrics, which represent the various columns and computed values within your database.

**Defining a Table:**

```typescript
const customersTable = C.table("Customer")
  .withDimension("customer_id", {
    type: "number",
    primaryKey: true,
    sql: ({ table, sql }) => sql`${table.column("CustomerId")}`,
  })
  .withDimension("first_name", {
    type: "string",
    sql: ({ table }) => table.column("FirstName"),
  })
  .withDimension("last_name", {
    type: "string",
    sql: ({ table }) => table.column("LastName"),
  });

const invoicesTable = C.table("Invoice")
  .withDimension("invoice_id", {
    type: "number",
    primaryKey: true,
    sql: ({ table, sql }) => sql`${table.column("InvoiceId")}`,
  })
  .withMetric("total", {
    type: "sum",
    sql: ({ table }) => table.column("Total"),
  });
```

**Defining a Database and joining tables:**

```typescript
const db = C.database()
  .withTable(customersTable)
  .withTable(invoicesTable)
  .joinOneToMany(
    "Customer",
    "Invoice",
    ({ sql, dimensions }) =>
      sql`${dimensions.Customer.customer_id} = ${dimensions.Invoice.customer_id}`
  );
```

### Data Querying

Leverage the library's querying capabilities to fetch dimensions and metrics, apply filters, and sort results efficiently.

```typescript
// Dimension and metric query
const query = db.query({
  dimensions: ["Customer.customer_id"],
  metrics: ["Invoice.total"],
  order: { "Customer.customer_id": "asc" },
  limit: 10,
});

// Metric query with filters
const query = db.query({
  metrics: ["Invoice.total", "InvoiceLine.quantity"],
  filters: [{ operator: "equals", member: "Customer.customer_id", value: [1] }],
});

// Dimension query with filters
const query = db.query({
  dimensions: ["Customer.first_name", "Customer.last_name"],
  filters: [{ operator: "equals", member: "Customer.customer_id", value: [1] }],
});

// Filtering and sorting
const query = db.query({
  dimensions: ["Customer.first_name"],
  metrics: ["Invoice.total"],
  filters: [{ operator: "gt", member: "Invoice.total", value: [100] }],
  order: { "Invoice.total": "desc" },
});
```

### Executing queries

Note: @verybigthings/semantic-layer focuses on SQL generation. Execute the generated queries with your SQL client:

```typescript
const result = await sqlClient.query(query.sql, query.bindings);
```

## Acknowledgments

@verybigthings/semantic-layer draws inspiration from several BI libraries, particularly [https://cube.dev](Cube.dev). While our API is very close to that of Cube.dev, future development may change our approach.
