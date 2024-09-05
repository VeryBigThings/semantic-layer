---
sidebar_position: 1
slug: /
---

# Quick Start Guide

Welcome to the semantic layer library! Let's dive in and create a simple data model in just a few steps.

## Installation

First, let's get the library installed:

```bash npm2yarn
npm install @verybigthings/semantic-layer
```

## Building Your First Semantic Layer

Imagine you're running a music store. You have customers, and they make purchases. Let's model this!

### Step 1: Create Your Models

We'll create two models: `customers` and `invoices`.

```typescript
import * as semanticLayer from "@verybigthings/semantic-layer";

// Our Customers model
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
  })
  .withDimension("last_name", {
    type: "string",
    sql: ({ model }) => model.column("LastName"),
  });

// Our Invoices model
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
  })
  .withMetric("total", {
    type: "number",
    description: "Invoice total.",
    sql: ({ model, sql }) => sql`SUM(COALESCE(${model.column("Total")}, 0))`,
  });
```

### Step 2: Create a Repository

Now, let's put these models together in a repository:

```typescript
const repository = semanticLayer
  .repository()
  .withModel(customersModel)
  .withModel(invoicesModel)
  .joinOneToMany(
    "customers",
    "invoices",
    ({ sql, models }) =>
      sql`${models.customers.dimension(
        "customer_id"
      )} = ${models.invoices.dimension("customer_id")}`
  );
```

### Step 3: Build a Query

With our repository set up, we can now build queries:

```typescript
const queryBuilder = repository.build("postgresql");

const query = queryBuilder.buildQuery({
  members: [
    "customers.customer_id",
    "customers.first_name",
    "customers.last_name",
    "invoices.total",
  ],
  order: { "customers.customer_id": "asc" },
  limit: 10,
});
```

### Step 4: Execute the Query

The `query` object contains the SQL string and bindings. You can use these with your preferred database client:

```typescript
const result = await someSqlClient.query(query.sql, query.bindings);
```

For example, with the `pg` package for PostgreSQL:

```typescript
const result = await pg.query(query.sql, query.bindings);
```

And there you have it! You've just set up a semantic layer for your music store data. This layer will make it easy to analyze customer purchases without writing complex SQL queries each time.
