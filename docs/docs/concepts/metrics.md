---
sidebar_position: 4
---

# Metrics

In the world of data analysis, metrics are crucial for quantifying and measuring various aspects of your business. Let's explore how to create and use metrics effectively in the `@verybigthings/semantic-layer` library.

## What is a Metric?

A metric is a quantifiable measure used to track and assess a specific aspect of your data. It's the numerical representation that helps you understand performance, trends, and patterns in your data.

## Creating Your First Metric

Let's continue with our online bookstore example. We'll create metrics for both our customers and invoices:

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
  })
  .withMetric("count", {
    type: "number",
    sql: ({ model, sql }) => sql`COUNT(DISTINCT ${model.column("CustomerId")})`,
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
  })
  .withMetric("total", {
    type: "number",
    sql: ({ model, sql }) => sql`SUM(COALESCE(${model.column("Total")}, 0))`,
  });
```

In these examples, we've created two metrics:

1. `count`: Counts the number of unique customers in our database.
2. `total`: Calculates the total amount of money spent by customers.

## Metric Properties

Let's examine the key properties of metrics:

1. **Type**: Specifies the data type of your metric. Options include:

   - `string`
   - `number`
   - `boolean`
   - `datetime`
   - `date`
   - `time`

2. **Description**: Provides context about what the metric represents.

3. **Format**: Allows you to customize how the metric is displayed:

```typescript
.withMetric("count", {
  type: "number",
  sql: ({ model, sql }) => sql`COUNT(DISTINCT ${model.column("CustomerId")})`,
  format: (value) => `${value} customers`,
})
```

4. **Private**: Set to `true` to hide the metric from public view while still allowing it to be referenced in other metrics.

5. **SQL**: A required property for all metrics. It's a function that returns a SQL fragment representing the metric calculation. It takes an object with the following properties:

- `model`: The model the dimension belongs to with the following properties:
  - `column`: A function that returns a reference to a column
  - `dimension`: A function that returns a reference to a dimension
  - `metric`: A function that returns a reference to a metric
- `sql`: A function that returns a SQL fragment
- `getContext`: A function that returns the current context
- `identifier`: A function that returns a SQL identifier

## Crafting Custom Metric Definitions

When defining metrics, remember that the SQL fragment must be a valid SQL aggregation function. For example:

```typescript
// Valid metric definition
.withMetric("count", {
  type: "number",
  sql: ({ model, sql }) => sql`COUNT(DISTINCT ${model.column("CustomerId")})`,
})

// Invalid metric definition (no aggregation function)
.withMetric("count", {
  type: "number",
  sql: ({ model, sql }) => sql`${model.column("CustomerId")}`,
})
```

## Advanced Metric Calculations

Metrics can reference not only columns and dimensions but also other metrics. This allows for complex calculations, such as calculating aggregates of aggregates. Here's an example:

```typescript
const storeSalesModel = semanticLayer
  .model()
  .withName("store_sales")
  .fromSqlQuery(
    ({ sql }) =>
      sql`SELECT 1 AS id, 1 AS store_id, 1 AS product_id, 10 AS sales UNION ALL
      SELECT 2 AS id, 1 AS store_id, 1 AS product_id, 20 AS sales UNION ALL
      SELECT 3 AS id, 1 AS store_id, 2 AS product_id, 30 AS sales UNION ALL
      SELECT 4 AS id, 1 AS store_id, 2 AS product_id, 40 AS sales UNION ALL
      SELECT 5 AS id, 2 AS store_id, 1 AS product_id, 50 AS sales UNION ALL
      SELECT 6 AS id, 2 AS store_id, 1 AS product_id, 60 AS sales UNION ALL
      SELECT 7 AS id, 2 AS store_id, 2 AS product_id, 70 AS sales UNION ALL
      SELECT 8 AS id, 2 AS store_id, 2 AS product_id, 80 AS sales`
  )
  .withDimension("id", {
    type: "number",
    sql: ({ model, sql }) => sql`${model.column("id")}`,
  })
  .withDimension("store_id", {
    type: "number",
    sql: ({ model, sql }) => sql`${model.column("store_id")}`,
    primaryKey: true,
  })
  .withDimension("product_id", {
    type: "number",
    sql: ({ model, sql }) => sql`${model.column("product_id")}`,
    primaryKey: true,
  })
  .withMetric("sales", {
    type: "number",
    sql: ({ model, sql }) => sql`SUM(${model.column("sales")})`,
  })
  .withMetric("median_sales", {
    type: "number",
    sql: ({ model, sql }) =>
      sql`PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ${model.metric(
        "sales"
      )})`,
  });
```

_Note: this example is based on the Cube example that can be found [here](https://cube.dev/docs/guides/recipes/data-modeling/nested-aggregates)._

In this example, we've created a `median_sales` metric that calculates the median of the `sales` metric. This demonstrates how you can use one metric as input for another, allowing for sophisticated analytical calculations.

In some cases, you might want to reference a column or a dimension that will not be used in the aggregation function. In that case, you can call the `groupBy` function on a dimension or column reference:

```typescript
.withMetric("avg_daily_sales", {
  type: "number",
  sql: ({ model, sql }) =>
    sql`SUM(${model.column("sales")}) / ${model.dimension("days_in_period").groupBy()}`,
});
```

Note that in this case, the `avg_daily_sales` metric will be grouped by the `days_in_period` which means that additional rows might be added to the result set.
