---
sidebar_position: 3
---

# Dimensions

Imagine you're organizing your bookshelf. Each book has attributes like title, author, genre, and publication year. In the world of data, these attributes are what we call dimensions. They're the properties that help you slice, dice, and group your data in meaningful ways.

## Creating Your First Dimension

Let's stick with our online bookstore example. Here's how you might create dimensions for your customer data:

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

In this example, we've created three dimensions: `customer_id`, `first_name`, and `last_name`. Each one represents a different piece of information about our customers.

## Dimension Properties

Let's look at the dimension properties:

1. **Type**: This is the data type of your dimension. Options include:

   - `string`: For text (like names)
   - `number`: For numeric values (like IDs or ages)
   - `boolean`: For true/false values
   - `datetime`: For date and time
   - `date`: For just the date
   - `time`: For just the time

2. **Primary Key**: This is like the VIP pass of dimensions. Set `primaryKey: true` for the dimension that uniquely identifies each record.

3. **Description**: A helpful note to yourself (or others) about what this dimension represents.

4. **Format**: Want to present your data in a specific way? Use the `format` property:

```typescript
.withDimension("first_name", {
  type: "string",
  sql: ({ model }) => model.column("FirstName"),
  format: (value) => `${value?.toUpperCase()}`,
})
```

5. **Private**: Need to keep a dimension under wraps? Set `private: true` to hide it from public view. In this case dimension can still be referenced from other dimensions and metrics, but it won't be visible in the query schema.

6. **SQL**: Want to define a custom SQL logic for your dimension? Use the `sql` property. It's a function that takes an object with the following properties:

- `model`: The model the dimension belongs to with the following properties:
  - `column`: A function that returns a reference to a column
  - `dimension`: A function that returns a reference to a dimension
- `sql`: A function that returns a SQL fragment
- `getContext`: A function that returns the current context
- `identifier`: A function that returns a SQL identifier

## Custom Dimension Definitions

Sometimes, you need a bit more flexibility in defining your dimensions. That's where the `sql` property comes in handy. It's like having a magic wand for your data!

### Simple Column Reference

```typescript
.withDimension("first_name", {
  type: "string",
  sql: ({ model }) => model.column("FirstName"),
})
```

### Custom SQL Logic

```typescript
.withDimension("is_adult", {
  type: "boolean",
  sql: ({ model, sql }) => sql`${model.column("Age")} >= 18`,
})
```

### Using Context for Dynamic Queries

```typescript
const customersModel = semanticLayer
  .model<{ adultAge: number }>()
  .withName("customers")
  .fromTable("Customer")
  .withDimension("is_adult", {
    type: "boolean",
    sql: ({ model, sql, getContext }) =>
      sql`${model.column("Age")} >= ${getContext().adultAge}`,
  });
```

### Combining Dimensions

```typescript
.withDimension("full_name", {
  type: "string",
  sql: ({ model, sql }) =>
    sql`${model.dimension("first_name")} || ' ' || ${model.dimension("last_name")}`,
})
```

## Quick and Easy Dimension Definitions

For simple cases, you can skip the `sql` property altogether:

```typescript
const customersModel = semanticLayer
  .model()
  .withName("customers")
  .fromTable("Customer")
  .withDimension("customer_id", {
    type: "number",
    primaryKey: true,
  })
  .withDimension("first_name", {
    type: "string",
  })
  .withDimension("last_name", {
    type: "string",
  });
```

In this case, the library assumes the dimension name matches the column name in your database.
