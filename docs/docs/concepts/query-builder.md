---
sidebar_position: 6
---

# Query Builder

## Introduction to Query Builder

The Query Builder is a core component of the semantic layer, providing a flexible and powerful interface for constructing complex queries based on your data models. It abstracts the intricacies of SQL, allowing you to focus on the business logic of your data analysis.

## Creating a Query Builder

To create a Query Builder, you need a defined repository and a specified dialect. Here's a basic example:

```typescript
import * as semanticLayer from "@verybigthings/semantic-layer";

// Assume models (customersModel, invoicesModel, invoiceLinesModel) are defined here

const repository = semanticLayer
  .repository()
  .withModel(customersModel)
  .withModel(invoicesModel)
  .withModel(invoiceLinesModel);
// Assume joins are defined here

const queryBuilder = repository.build("postgresql");
```

## Constructing Queries

The Query Builder generates SQL from semantic layer queries. A basic query structure looks like this:

```typescript
const query = queryBuilder.buildQuery({
  members: ["customers.customer_id", "invoices.total"],
  order: { "customers.customer_id": "asc" },
  limit: 10,
});
```

This query selects `customer_id` and `total` from the `customers` and `invoices` models, orders by `customer_id` ascending, and limits to 10 results.

## Query Structure and Type Inference

The `query` object returned by `buildQuery` has the following structure:

```typescript
{
  sql: string;
  bindings: unknown[];
}
```

To infer the return type of your query, use the `InferSqlQueryResultType` type:

```typescript
type QueryResult = semanticLayer.InferSqlQueryResultType<typeof query>;
```

For inferring the return type from a query object without calling `buildQuery`, use `QueryReturnType`:

```typescript
const queryObject = {
  members: ["customers.customer_id", "customers.first_name"],
  order: [{ member: "customers.customer_id", direction: "asc" }],
  filters: [
    { operator: "equals", member: "customers.customer_id", value: [1] },
  ],
  limit: 10,
} satisfies semanticLayer.QueryBuilderQuery<typeof queryBuilder>;

type QueryResult = semanticLayer.InferSqlQueryResultTypeFromQuery<
  typeof queryBuilder,
  typeof queryObject
>;
```

## Query Properties

The Query Builder supports several properties to define your query:

1. **members**: An array of strings representing the dimensions and metrics to include.
2. **order**: Specifies the ordering of results.
3. **limit**: Sets the maximum number of results to return.
4. **offset**: Determines the number of results to skip.
5. **filters**: An array of filter objects to apply to the query.

## Filter Types

The Query Builder supports a wide range of filter operators, including:

- Logical operators: `and`, `or`
- Equality operators: `equals`, `notEquals`, `in`, `notIn`
- Null checks: `set`, `notSet`
- String operators: `contains`, `notContains`, `startsWith`, `notStartsWith`, `endsWith`, `notEndsWith`
- Numeric comparisons: `gt`, `gte`, `lt`, `lte`
- Date range operators: `inDateRange`, `notInDateRange`, `beforeDate`, `afterDate`
- Subquery operators: `inQuery`, `notInQuery`

Example of a filter:

```typescript
{
  operator: "equals",
  member: "albums.name",
  value: ["Thriller"]
}
```

### Filter Evaluation

It's important to note that dimension filters are evaluated before data grouping and aggregation, while metric filters are evaluated after. This means metric and dimension filters cannot be used in the same `and` or `or` connective.

## Filter Types in Detail

The Query Builder supports a wide range of filter operators to refine your queries. Here's a detailed breakdown of each filter type:

### Logical Operators

1. **and**: Connects two or more filters with a logical AND.

   ```typescript
   {
     operator: "and",
     filters: [
       { operator: "equals", member: "albums.name", value: ["Thriller"] },
       { operator: "gt", member: "tracks.duration", value: [300] }
     ]
   }
   ```

2. **or**: Connects two or more filters with a logical OR.
   ```typescript
   {
     operator: "or",
     filters: [
       { operator: "equals", member: "albums.name", value: ["Thriller"] },
       { operator: "equals", member: "albums.name", value: ["Bad"] }
     ]
   }
   ```

### Equality Operators

3. **equals**: Filters values that are equal to the given value.

   ```typescript
   {
     operator: "equals",
     member: "albums.name",
     value: ["Thriller"]
   }
   ```

4. **notEquals**: Filters values that are not equal to the given value.

   ```typescript
   {
     operator: "notEquals",
     member: "albums.name",
     value: ["Thriller"]
   }
   ```

5. **in**: Filters values that are in the given array of values.

   ```typescript
   {
     operator: "in",
     member: "albums.name",
     value: ["Thriller", "Bad", "Dangerous"]
   }
   ```

6. **notIn**: Filters values that are not in the given array of values.
   ```typescript
   {
     operator: "notIn",
     member: "albums.name",
     value: ["Thriller", "Bad", "Dangerous"]
   }
   ```

### Null Checks

7. **set**: Filters values that are set (not null).

   ```typescript
   {
     operator: "set",
     member: "albums.release_date"
   }
   ```

8. **notSet**: Filters values that are not set (null).
   ```typescript
   {
     operator: "notSet",
     member: "albums.release_date"
   }
   ```

### String Operators

9. **contains**: Filters values that contain the given string.

   ```typescript
   {
     operator: "contains",
     member: "tracks.name",
     value: ["Love"]
   }
   ```

10. **notContains**: Filters values that do not contain the given string.

    ```typescript
    {
      operator: "notContains",
      member: "tracks.name",
      value: ["Love"]
    }
    ```

11. **startsWith**: Filters values that start with the given string.

    ```typescript
    {
      operator: "startsWith",
      member: "tracks.name",
      value: ["The"]
    }
    ```

12. **notStartsWith**: Filters values that do not start with the given string.

    ```typescript
    {
      operator: "notStartsWith",
      member: "tracks.name",
      value: ["The"]
    }
    ```

13. **endsWith**: Filters values that end with the given string.

    ```typescript
    {
      operator: "endsWith",
      member: "tracks.name",
      value: ["Song"]
    }
    ```

14. **notEndsWith**: Filters values that do not end with the given string.
    ```typescript
    {
      operator: "notEndsWith",
      member: "tracks.name",
      value: ["Song"]
    }
    ```

### Numeric Comparisons

15. **gt**: Filters values that are greater than the given value.

    ```typescript
    {
      operator: "gt",
      member: "tracks.duration",
      value: [300]
    }
    ```

16. **gte**: Filters values that are greater than or equal to the given value.

    ```typescript
    {
      operator: "gte",
      member: "tracks.duration",
      value: [300]
    }
    ```

17. **lt**: Filters values that are less than the given value.

    ```typescript
    {
      operator: "lt",
      member: "tracks.duration",
      value: [300]
    }
    ```

18. **lte**: Filters values that are less than or equal to the given value.
    ```typescript
    {
      operator: "lte",
      member: "tracks.duration",
      value: [300]
    }
    ```

### Date Range Operators

19. **inDateRange**: Filters dates that are in the given range.

    ```typescript
    {
      operator: "inDateRange",
      member: "albums.release_date",
      value: { startDate: "2022-01-01", endDate: "2022-12-31" }
    }
    ```

20. **notInDateRange**: Filters dates that are not in the given range.

    ```typescript
    {
      operator: "notInDateRange",
      member: "albums.release_date",
      value: { startDate: "2022-01-01", endDate: "2022-12-31" }
    }
    ```

21. **beforeDate**: Filters dates that are before the given date.

    ```typescript
    {
      operator: "beforeDate",
      member: "albums.release_date",
      value: "2022-01-01"
    }
    ```

22. **afterDate**: Filters dates that are after the given date.
    ```typescript
    {
      operator: "afterDate",
      member: "albums.release_date",
      value: "2022-01-01"
    }
    ```

### Subquery Operators

23. **inQuery**: Filters values that are in the result of the given query.

    ```typescript
    {
      operator: "inQuery",
      member: "customer.customer_id",
      value: {
        members: ["customer.customer_id"],
        filters: [
          {
            operator: "equals",
            member: "invoices.invoice_id",
            value: ["1"]
          }
        ]
      }
    }
    ```

24. **notInQuery**: Filters values that are not in the result of the given query.
    ```typescript
    {
      operator: "notInQuery",
      member: "customer.customer_id",
      value: {
        members: ["customer.customer_id"],
        filters: [
          {
            operator: "equals",
            member: "invoices.invoice_id",
            value: ["1"]
          }
        ]
      }
    }
    ```

### Important Notes on Filter Usage

1. For operators that accept multiple values (like `in`, `notIn`), the value should be an array.
2. Date values should always be in ISO 8601 format for consistency.
3. The `inQuery` and `notInQuery` operators accept a full query object as their value, allowing for complex nested queries.
4. Remember that dimension filters are evaluated before data grouping and aggregation, while metric filters are evaluated after. This means metric and dimension filters cannot be used in the same `and` or `or` connective.

By leveraging these filter types effectively, you can create highly specific and powerful queries to extract precisely the data you need from your semantic layer.

## Query Examples

1. Querying multiple dimensions:

```typescript
const query = queryBuilder.buildQuery({
  members: ["albums.name", "customers.full_name"],
  order: { "albums.name": "asc" },
});
```

2. Querying a metric sliced across two dimensions:

```typescript
const query = queryBuilder.buildQuery({
  members: ["customers.name", "genres.name", "tracks.count"],
  order: { "customers.name": "asc" },
});
```

3. Filtering results by another query's result:

```typescript
const query = queryBuilder.buildQuery({
  members: ["customer.customer_id", "invoices.invoice_count"],
  filters: [
    {
      operator: "inQuery",
      member: "customer.customer_id",
      value: {
        members: ["customer.customer_id"],
        filters: [
          {
            operator: "equals",
            member: "invoices.invoice_id",
            value: ["1"],
          },
        ],
      },
    },
  ],
});
```

## Best Practices

1. Use meaningful names for your dimensions and metrics to make queries more readable.
2. Leverage type inference to catch potential errors early in development.
3. Be mindful of the difference between dimension and metric filters when constructing complex queries.
4. Use the appropriate filter operators to optimize query performance.
5. When working with date ranges, ensure your dates are in ISO 8601 format for consistency.

By mastering the Query Builder, you can efficiently extract valuable insights from your data while maintaining a clean separation between your business logic and the underlying SQL complexity.
