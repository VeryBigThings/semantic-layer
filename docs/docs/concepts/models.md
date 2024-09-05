---
sidebar_position: 2
---

# Models

Models are the building blocks of your semantic layer. They represent the structure and relationships of your data. Let's explore how to create them.

## What's a Model?

Think of a model as a blueprint for your data. It defines what data you have and how it's organized. In our library, you can create models from database tables or custom SQL queries.

## Creating Models from Database Tables

Let's say you're running an online bookstore. You might have a `Customers` table in your database. Here's how you'd create a model for it:

```typescript
import * as semanticLayer from "@verybigthings/semantic-layer";

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
```

In this example, we're creating a `customers` model based on the `Customer` table. We've defined three dimensions: `customer_id` (which is also our primary key), `first_name`, and `last_name`.

## Creating Models from Custom SQL Queries

Sometimes, you might need more flexibility than a single table provides. That's where custom SQL queries come in handy. Let's create a model for "active customers":

```typescript
import * as semanticLayer from "@verybigthings/semantic-layer";

const activeCustomersModel = semanticLayer
  .model()
  .withName("active_customers")
  .fromSqlQuery(
    ({ sql, identifier }) => sql`
      SELECT * FROM ${identifier("Customer")}
      WHERE ${identifier("LastPurchaseDate")} > CURRENT_DATE - INTERVAL '1 year'
    `
  )
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
```

Here, we're using a custom SQL query to select only customers who have made a purchase in the last year. The `sql` and `identifier` functions help us build safe, dynamic SQL queries.

## Adding Flexibility with Context

Want to make your models even more dynamic? You can use context to adjust your SQL queries on the fly. Here's an example:

```typescript
import * as semanticLayer from "@verybigthings/semantic-layer";

const flexibleCustomersModel = semanticLayer
  .model<{ isVIP: boolean }>()
  .withName("customers")
  .fromSqlQuery(
    ({ sql, identifier, getContext }) => sql`
      SELECT * FROM ${
        getContext().isVIP
          ? identifier("VIPCustomers")
          : identifier("RegularCustomers")
      }
    `
  )
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
```

In this example, we're using the `isVIP` context to determine which table to query. This allows you to use the same model for different scenarios.

## Remember: Always Define a Primary Key

Every model should have at least one primary key. This ensures that each row in your data can be uniquely identified. Primary keys are crucial for accurate metric calculations.
