---
sidebar_position: 5
---

# Repository

## Introduction to Repositories

In the semantic layer, a repository serves as the central component that defines the structure of your data and how it can be queried. It's a collection of interconnected models that forms the foundation of your data analysis capabilities.

## Creating Your First Repository

Let's create a repository for an online bookstore using three models: customers, invoices, and invoice lines.

```typescript
import * as semanticLayer from "@verybigthings/semantic-layer";

// Define customer model
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
  })
  .withMetric("count", {
    type: "number",
    sql: ({ model, sql }) => sql`COUNT(DISTINCT ${model.column("CustomerId")})`,
  });

// Define invoice model
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
    sql: ({ model, sql }) => sql`SUM(COALESCE(${model.column("Total")}, 0))`,
  });

// Define invoice line model
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
  })
  .withMetric("quantity", {
    type: "number",
    sql: ({ model, sql }) => sql`SUM(COALESCE(${model.column("Quantity")}, 0))`,
  })
  .withMetric("unit_price", {
    type: "number",
    sql: ({ model, sql }) =>
      sql`SUM(COALESCE(${model.column("UnitPrice")}, 0))`,
  });

// Create the repository
const repository = semanticLayer
  .repository()
  .withModel(customersModel)
  .withModel(invoicesModel)
  .withModel(invoiceLinesModel)
  .joinOneToMany(
    "customers",
    "invoices",
    ({ sql, models }) =>
      sql`${models.customers.dimension(
        "customer_id"
      )} = ${models.invoices.dimension("customer_id")}`
  )
  .joinOneToMany(
    "invoices",
    "invoice_lines",
    ({ sql, models }) =>
      sql`${models.invoices.dimension(
        "invoice_id"
      )} = ${models.invoice_lines.dimension("invoice_id")}`
  );
```

This example demonstrates how to create a repository with three interconnected models: customers, invoices, and invoice lines.

## Key Components of a Repository

1. **Models**: The foundation of your repository. Each model represents a distinct entity in your data structure.
2. **Joins**: Define the relationships between your models, allowing for complex queries across multiple entities.

Note: All models in a repository must be connected through joins to ensure data integrity and enable comprehensive querying.

## Defining Joins

Joins are crucial for establishing relationships between your models. The semantic layer library supports four types of joins:

1. **One-to-One Joins**
2. **One-to-Many Joins**
3. **Many-to-One Joins**
4. **Many-to-Many Joins**

Example of a one-to-many join:

```typescript
.joinOneToMany(
  "customers",
  "invoices",
  ({ sql, models }) =>
    sql`${models.customers.dimension("customer_id")} = ${models.invoices.dimension("customer_id")}`
)
```

By default, all joins are treated as LEFT JOINs, where the first model is the left side and the second model is the right side.

## Optimizing Joins

### Join Priority

You can optimize query performance (or ensure query correctness) by setting join priorities:

```typescript
.joinOneToMany(
  "customers",
  "invoices",
  ({ sql, models }) =>
    sql`${models.customers.dimension("customer_id")} = ${models.invoices.dimension("customer_id")}`,
  { priority: "high" }
)
```

Priority options include `low`, `normal` (default), and `high`.

### Join Type

You can explicitly define join types as `inner` or `full`:

```typescript
.joinOneToMany(
  "customers",
  "invoices",
  ({ sql, models }) =>
    sql`${models.customers.dimension("customer_id")} = ${models.invoices.dimension("customer_id")}`,
  { type: "full" }
)
```

## Advanced Features

### Calculated Dimensions

Calculated dimensions allow you to create new dimensions based on data from multiple models (which don't have to be directly connected):

```typescript
.withCalculatedDimension("album.artist_name_and_title", {
  type: "string",
  sql: ({ sql, models }) =>
    sql`${models.artists.dimension("name")} || ': ' || ${models.albums.dimension("title")}`,
})
```

### Calculated Metrics

Calculated metrics enable you to create new metrics based on data from multiple models (which don't have to be directly connected):

```typescript
.withCalculatedMetric("invoice_lines.ratio_of_total", {
  type: "number",
  sql: ({ sql, models }) =>
    sql`${models.invoice_lines.metric("total")} / NULLIF(${models.invoices.metric("total")}, 0)`,
})
```

Note: In calculated metrics, we don't use the `aggregated()` function unless we're specifically working with pre-aggregated values. The semantic layer will handle the appropriate level of aggregation based on the selected dimensions.

## Understanding Aggregation in Calculated Metrics

When working with calculated metrics, it's crucial to understand when to use the `aggregated()` function:

1. Without `aggregated()`:

   - Primary key dimensions from the metric's model are automatically projected.
   - This may result in row multiplication based on the dimensions selected by the user.
   - Allows for additional levels of aggregation or calculations on the raw data.

2. With `aggregated()`:
   - Metrics are pre-calculated and sliced based on the dimensions selected by the user.
   - No row multiplication will occur.
   - Further aggregation would simply return the same value.

Example with `aggregated()` (for a percentage calculation):

```typescript
.withCalculatedMetric("sales.percent_of_total", {
  type: "number",
  sql: ({ sql, models }) => sql`
    ${models.sales.metric("total").aggregated()} /
    NULLIF(${models.sales.metric("grand_total").aggregated()}, 0) * 100
  `,
})
```

## Best Practices

1. Ensure all models in your repository are connected through joins.
2. Use appropriate join types and priorities to optimize query performance.
3. Leverage calculated dimensions and metrics to create complex, cross-model data points.
4. Be mindful of when to use the `aggregated()` function in calculated metrics.
5. Document your repository structure, including model relationships and any calculated fields.
6. Regularly review and optimize your repository based on query performance and evolving business needs.

By mastering repositories, you'll be able to create a robust and flexible semantic layer that accurately represents your data structure and enables powerful, efficient querying.
