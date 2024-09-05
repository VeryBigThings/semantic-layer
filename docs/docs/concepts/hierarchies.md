---
sidebar_position: 7
---

# Hierarchies

## Introduction

Hierarchies are a powerful feature in the `@verybigthings/semantic-layer` library that allow you to organize and group your data in a structured, multi-level format. They are essential for creating sophisticated visual representations of your data and facilitating in-depth data analysis. This document will guide you through the process of creating and utilizing hierarchies within your semantic layer models and repositories.

## Types of Hierarchies

The library supports two primary types of hierarchies:

1. **Categorical Hierarchies**: These organize data into groups based on specific dimensions. They are particularly useful for creating visual representations of your data that allow users to drill down from broad categories to more specific details.

2. **Temporal Hierarchies**: These organize data into groups based on temporal dimensions (date, datetime, or time). They allow for time-based analysis at various levels of granularity (e.g., year, quarter, month, week, day).

## Creating Hierarchies

Hierarchies can be defined at both the model and repository levels. Let's explore how to create hierarchies with a comprehensive example.

### Step 1: Define Your Models

First, let's define some models that we'll use to create our hierarchies. We'll use a music database as an example.

```typescript
import * as semanticLayer from "@verybigthings/semantic-layer";

// Customers Model
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
  .withDimension("full_name", {
    type: "string",
    sql: ({ model, sql }) =>
      sql`${model.dimension("first_name")} || ' ' || ${model.dimension(
        "last_name"
      )}`,
  })
  .withDimension("email", {
    type: "string",
    sql: ({ model }) => model.column("Email"),
  })
  .withMetric("count", {
    type: "number",
    sql: ({ model, sql }) => sql`COUNT(DISTINCT ${model.column("CustomerId")})`,
  })
  .withCategoricalHierarchy("personal_information", ({ element }) => [
    element("personal_information")
      .withDimensions([
        "customer_id",
        "first_name",
        "last_name",
        "full_name",
        "email",
      ])
      .withKey(["customer_id"])
      .withFormat(["full_name"]),
  ]);

// Invoices Model
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
  .withDimension("invoice_date", {
    type: "date",
    sql: ({ model }) => model.column("InvoiceDate"),
  })
  .withMetric("total", {
    type: "number",
    description: "Invoice total.",
    sql: ({ model, sql }) => sql`SUM(COALESCE(${model.column("Total")}, 0))`,
    format: "currency",
  });

// Tracks Model
const tracksModel = semanticLayer
  .model()
  .withName("tracks")
  .fromTable("Track")
  .withDimension("track_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model }) => model.column("TrackId"),
  })
  .withDimension("name", {
    type: "string",
    sql: ({ model }) => model.column("Name"),
  })
  .withDimension("album_id", {
    type: "number",
    sql: ({ model }) => model.column("AlbumId"),
  })
  .withDimension("genre_id", {
    type: "number",
    sql: ({ model }) => model.column("GenreId"),
  })
  .withMetric("length", {
    type: "number",
    sql: ({ model, sql }) =>
      sql`SUM(COALESCE(${model.column("Milliseconds")}, 0))`,
    format: (value) => `${value} milliseconds`,
  });

// Albums Model
const albumsModel = semanticLayer
  .model()
  .withName("albums")
  .fromTable("Album")
  .withDimension("album_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model }) => model.column("AlbumId"),
  })
  .withDimension("title", {
    type: "string",
    sql: ({ model }) => model.column("Title"),
  })
  .withDimension("artist_id", {
    type: "number",
    sql: ({ model }) => model.column("ArtistId"),
  });

// Artists Model
const artistsModel = semanticLayer
  .model()
  .withName("artists")
  .fromTable("Artist")
  .withDimension("artist_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model }) => model.column("ArtistId"),
  })
  .withDimension("name", {
    type: "string",
    sql: ({ model }) => model.column("Name"),
  })
  .withCategoricalHierarchy("artist", ({ element }) => [
    element("artist")
      .withDimensions(["artist_id", "name"])
      .withKey(["artist_id"])
      .withFormat(["name"]),
  ]);

// Genres Model
const genresModel = semanticLayer
  .model()
  .withName("genres")
  .fromTable("Genre")
  .withDimension("genre_id", {
    type: "number",
    primaryKey: true,
    sql: ({ model }) => model.column("GenreId"),
  })
  .withDimension("name", {
    type: "string",
    sql: ({ model }) => model.column("Name"),
  })
  .withCategoricalHierarchy("genre", ({ element }) => [
    element("genre")
      .withDimensions(["genre_id", "name"])
      .withKey(["genre_id"])
      .withFormat(["name"]),
  ]);
```

### Step 2: Create a Repository and Define Joins

Next, we'll create a repository that includes all our models and defines the relationships between them:

```typescript
const repository = semanticLayer
  .repository()
  .withModel(customersModel)
  .withModel(invoicesModel)
  .withModel(tracksModel)
  .withModel(albumsModel)
  .withModel(artistsModel)
  .withModel(genresModel)
  .joinOneToMany(
    "customers",
    "invoices",
    ({ sql, models }) =>
      sql`${models.customers.dimension(
        "customer_id"
      )} = ${models.invoices.dimension("customer_id")}`
  )
  .joinManyToOne(
    "tracks",
    "albums",
    ({ sql, models }) =>
      sql`${models.tracks.dimension("album_id")} = ${models.albums.dimension(
        "album_id"
      )}`
  )
  .joinManyToOne(
    "albums",
    "artists",
    ({ sql, models }) =>
      sql`${models.albums.dimension("artist_id")} = ${models.artists.dimension(
        "artist_id"
      )}`
  )
  .joinOneToOne(
    "tracks",
    "genres",
    ({ sql, models }) =>
      sql`${models.tracks.dimension("genre_id")} = ${models.genres.dimension(
        "genre_id"
      )}`
  );
```

### Step 3: Define Repository-Level Hierarchies

Now, let's define a repository-level hierarchy that spans multiple models:

```typescript
repository.withCategoricalHierarchy("music", ({ element }) => [
  element("artist")
    .withDimensions(["artists.artist_id", "artists.name"])
    .withKey(["artists.artist_id"])
    .withFormat(["artists.name"]),
  element("album")
    .withDimensions(["albums.album_id", "albums.title"])
    .withKey(["albums.album_id"])
    .withFormat(["albums.title"]),
  element("track")
    .withDimensions(["tracks.track_id", "tracks.name"])
    .withKey(["tracks.track_id"])
    .withFormat(["tracks.name"]),
]);
```

This "music" hierarchy allows users to drill down from artists to albums to individual tracks.

## Analyzing Queries with Hierarchies

Once you have defined your hierarchies, you can use them to analyze queries and provide users with the ability to drill down into the data. Here's how you can do this:

### Step 1: Build a Query

First, let's build a query using our repository:

```typescript
const queryBuilder = repository.build("postgresql");

const query = {
  members: [
    "artists.name",
    "albums.title",
    "tracks.name",
    "genres.name",
    "invoices.total",
  ],
  filters: [
    {
      operator: "equals",
      member: "genres.name",
      value: ["Rock"],
    },
  ],
  order: [
    {
      member: "artists.name",
      direction: "asc",
    },
  ],
};
```

### Step 2: Analyze the Query

Now, let's analyze this query to identify the hierarchies present:

```typescript
import { analyzer } from "@verybigthings/semantic-layer";

const queryAnalysis = analyzer.analyzeQuery(queryBuilder, query);
```

The `queryAnalysis` object will contain information about the hierarchies present in the query, including the levels of each hierarchy that are represented.

### Step 3: Analyze a Specific Hierarchy

If you want to analyze the query for a specific hierarchy, you can use the `analyzeQueryHierarchy` function:

```typescript
const musicHierarchy = queryAnalysis.hierarchies.categorical.find(
  ({ hierarchy }) => hierarchy.name === "music"
)?.hierarchy;

if (musicHierarchy) {
  const hierarchyAnalysis = analyzer.analyzeQueryHierarchy(
    queryAnalysis,
    musicHierarchy
  );

  console.log(JSON.stringify(hierarchyAnalysis, null, 2));
}
```

This will provide you with a detailed analysis of how the query relates to each level of the "music" hierarchy, including queries for each level of the hierarchy.

## Using Hierarchy Analysis for Drill-Down Functionality

The hierarchy analysis can be used to implement drill-down functionality in your application. Here's a basic example of how you might use this:

1. Present the user with the results of the original query.
2. When a user clicks on an item (e.g., an artist), use the `keyDimensions` from the corresponding level of the hierarchy to create a new query filter.
3. Add this filter to the query for the next level of the hierarchy.
4. Execute this new query to get the drill-down results.

Here's a conceptual example:

```typescript
function drillDown(hierarchyAnalysis, currentLevel, selectedItem) {
  const nextLevel = currentLevel + 1;
  if (nextLevel >= hierarchyAnalysis.queriesInfo.length) {
    console.log("Cannot drill down further");
    return;
  }

  const currentLevelInfo = hierarchyAnalysis.queriesInfo[currentLevel];
  const nextLevelInfo = hierarchyAnalysis.queriesInfo[nextLevel];

  // Create filters based on the selected item
  const newFilters = currentLevelInfo.keyDimensions.map((dimension) => ({
    operator: "equals",
    member: dimension,
    value: [selectedItem[dimension]],
  }));

  // Add these filters to the next level's query
  const drillDownQuery = {
    ...nextLevelInfo.query,
    filters: [...(nextLevelInfo.query.filters || []), ...newFilters],
  };

  // Execute this new query (implementation depends on your setup)
  executeQuery(drillDownQuery);
}
```

This function would be called when a user selects an item to drill down into, passing the hierarchy analysis, the current level, and the selected item.

## Conclusion

Hierarchies in the `@verybigthings/semantic-layer` library provide a powerful way to structure your data for analysis and visualization. By defining hierarchies at both the model and repository levels, you can create rich, multi-dimensional views of your data that allow users to explore from high-level overviews down to granular details.

Remember to consider your data structure and user needs when designing your hierarchies. Well-designed hierarchies can significantly enhance the usability and effectiveness of your data analysis tools.
