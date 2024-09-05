---
sidebar_position: 1
---

# Introduction

This document introduces key concepts of our `@verybigthings/semantic-layer` library.

## Semantic Layer

A semantic layer is an abstraction that sits between your raw data (often stored in databases or data warehouses) and the business users or applications that need to analyze this data. It provides a business-friendly, consistent, and governed way to access and interpret data across an organization.

Key benefits of a semantic layer include:

- Consistency: Ensures uniform definitions and calculations across different teams and tools
- Abstraction: Shields users from the complexity of underlying data structures
- Governance: Centralizes data access rules and security policies
- Reusability: Allows metrics and dimensions to be defined once and used many times

## Dimension

A dimension is a categorical attribute used to slice, dice, or group data in analysis. Dimensions provide context to your metrics and allow for more detailed and nuanced insights.

Examples of dimensions include:

- Date or time periods (e.g., year, month, day)
- Geographic locations (e.g., country, state, city)
- Product categories
- Customer segments

In our semantic layer library, dimensions are defined as properties that can be used to filter or group metrics.

## Metric

A metric is a quantitative measurement used to track and assess a business process or performance indicator. Metrics are typically numeric values that can be aggregated and analyzed over time or across different dimensions.

Examples of metrics include:

- Revenue
- Number of users
- Conversion rate
- Average order value

In our semantic layer library, metrics are defined as calculations or aggregations that can be performed on your data, often involving one or more dimensions.

By leveraging these concepts in your data model, you can create a powerful and flexible semantic layer that enables users to easily explore and analyze data without needing to understand the underlying data structures or write complex queries.
