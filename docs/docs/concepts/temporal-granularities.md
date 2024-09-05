---
sidebar_position: 8
---

# Temporal Granularities

## Introduction

Temporal granularities are a powerful feature in the `@verybigthings/semantic-layer` library that allow you to work with different levels of time-based data. This document will guide you through the process of using temporal granularities within your semantic layer models and queries.

## Understanding Temporal Granularities

The `@verybigthings/semantic-layer` library automatically creates temporal granularity dimensions for `date`, `datetime`, and `time` columns. These dimensions enable you to use specific parts of a temporal value, such as the year, quarter, or month. This functionality is particularly useful for creating less granular views of your data and for time-based analysis at various levels.

## Defining Temporal Dimensions

Let's start with an example of how to define a model with a temporal dimension:

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
  .withDimension("date_of_birth", {
    type: "date",
    sql: ({ model }) => model.column("DateOfBirth"),
  });
```

In this example, we've defined a `date_of_birth` dimension of type `date`. The library will automatically generate additional temporal granularity dimensions for this column.

## Generated Granularities

The specific granularities generated depend on the type of the temporal column. Here's a breakdown of the granularities created for each type:

### Date Granularities

For a `date` column, the following granularities are generated:

- `date_of_birth.year`
- `date_of_birth.quarter`
- `date_of_birth.quarter_of_year`
- `date_of_birth.month`
- `date_of_birth.month_num`
- `date_of_birth.week`
- `date_of_birth.week_num`
- `date_of_birth.day_of_month`

### Datetime Granularities

For a `datetime` column, these granularities are generated:

- `datetime_column.year`
- `datetime_column.quarter`
- `datetime_column.quarter_of_year`
- `datetime_column.month`
- `datetime_column.month_num`
- `datetime_column.week`
- `datetime_column.week_num`
- `datetime_column.day_of_month`
- `datetime_column.time`
- `datetime_column.hour`
- `datetime_column.hour_of_day`
- `datetime_column.minute`

### Time Granularities

For a `time` column, these granularities are generated:

- `time_column.hour`
- `time_column.minute`

## Temporal Hierarchies

When a temporal granularity is defined on a dimension, the library automatically creates a temporal hierarchy. This hierarchy allows for drilling down into the data by granularity. It's important to note that temporal hierarchies do not include all available granularities.

### Date Hierarchy

For `date` columns, the temporal hierarchy includes:

- year
- quarter
- month
- week

### Datetime Hierarchy

For `datetime` columns, the temporal hierarchy includes:

- year
- quarter
- month
- week
- date

Note that temporal hierarchies are not created for `time` dimensions.

## Omitting Granularity

In some cases, you may want to use a temporal column without generating additional granularities. You can achieve this by setting the `omitGranularity` property to `true` on the dimension:

```typescript
.withDimension("date_of_birth", {
  type: "date",
  sql: ({ model }) => model.column("DateOfBirth"),
  omitGranularity: true,
});
```

This will create a `date_of_birth` dimension that is a `date` column without any additional granularity dimensions. In this case, a temporal hierarchy will not be created.

## Using Temporal Granularities in Queries

Temporal granularities can be used in queries just like any other dimension. Here's an example of how to use temporal granularities in a query:

```typescript
const queryBuilder = repository.build("postgresql");

const query = queryBuilder.buildQuery({
  members: ["customers.date_of_birth.year", "customers.first_name"],
  order: { "customers.date_of_birth.year": "asc" },
  limit: 10,
});
```

In this query, we're selecting the year from the `date_of_birth` dimension along with the `first_name` dimension. We're also ordering the results by the birth year in ascending order and limiting the results to 10 rows.

## Best Practices for Using Temporal Granularities

1. **Choose the Right Granularity**: Select the granularity that best suits your analysis needs. For example, use `year` for long-term trends, `month` for seasonal patterns, or `day_of_month` for detailed daily analysis.

2. **Combine with Other Dimensions**: Temporal granularities are most powerful when combined with other dimensions. For example, you might analyze sales by product category and month.

3. **Use Hierarchies for Drill-Down**: Take advantage of the automatically generated temporal hierarchies to create interactive drill-down experiences in your applications.

4. **Consider Performance**: Be aware that querying at finer granularities (e.g., by day) will generally result in larger result sets and potentially slower queries compared to coarser granularities (e.g., by year).

5. **Handle Missing Data**: When working with temporal data, be prepared to handle missing data points. For example, if you're analyzing daily sales, there might be days with no sales recorded.

## Conclusion

Temporal granularities in the `@verybigthings/semantic-layer` library provide a powerful way to analyze time-based data at various levels of detail. By automatically generating these granularities and hierarchies, the library makes it easy to create sophisticated time-based analyses and visualizations.

Remember to consider the nature of your data and your analysis requirements when working with temporal granularities. Used effectively, they can provide valuable insights into trends, patterns, and changes over time in your data.
