import * as assert from "node:assert/strict";
import * as semanticLayer from "../index.js";

import { InferSqlQueryResultType, QueryBuilderQuery } from "../index.js";
import {
  PostgreSqlContainer,
  StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { after, before, describe, it } from "node:test";

import fs from "node:fs/promises";
import path from "node:path";
import pg from "pg";
import { zodToJsonSchema } from "zod-to-json-schema";

//import { format as sqlFormat } from "sql-formatter";

const __dirname = path.dirname(new URL(import.meta.url).pathname);

/*const query = built.query({
  dimensions: [
    "customers.customer_id",
    //'invoice_lines.invoice_line_id',
    //'invoices.invoice_id',
    //'Track.track_id,',
    //'albums.title',
  ],
  metrics: ["invoice_lines.total_unit_price", "invoices.total"],
  filters: [
    {
      operator: "inDateRange",
      member: "invoices.invoice_date",
      value: "from Jan 1st 2011 at 00:00 to Dec 31th 2012 23:00",
    },
    { operator: 'set', member: 'customers.customer_id' },
    {
      operator: 'notContains',
      member: 'invoice_lines.total_unit_price',
      value: ['0.99', '1'],
    },
    { operator: 'notEquals', member: 'invoices.total', value: ['0.99'] },
    {
      operator: 'or',
      filters: [
        { operator: 'notEquals', member: 'invoices.invoice_id', value: ['1'] },
        {
          operator: 'notEquals',
          member: 'invoice_lines.invoice_line_id',
          value: ['3'],
        },
      ],
    },
  ],
  order: {
    // 'invoice_lines.unit_price': 'asc',  'customers.customer_id': 'asc',
    "invoices.invoice_date.year": "desc",
  },
});*/

await describe("semantic layer", async () => {
  let container: StartedPostgreSqlContainer;
  let client: pg.Client;

  before(async () => {
    const bootstrapSql = await fs.readFile(
      path.join(__dirname, "Chinook_PostgreSql.sql"),
      "utf-8",
    );

    container = await new PostgreSqlContainer().start();

    client = new pg.Client({
      host: container.getHost(),
      port: container.getPort(),
      user: container.getUsername(),
      password: container.getPassword(),
      database: container.getDatabase(),
    });

    await client.connect();
    await client.query(bootstrapSql);
  });

  after(async () => {
    await client.end();
    await container.stop();
  });

  await describe("models from tables", async () => {
    const customersModel = semanticLayer
      .model("customers")
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
      .withDimension("company", {
        type: "string",
        sql: ({ model }) => model.column("Company"),
      })
      .withDimension("full_name", {
        type: "string",
        sql: ({ model, sql }) =>
          sql`${model.dimension("first_name")} || ' ' || ${model.dimension(
            "last_name",
          )}`,
      });

    const invoicesModel = semanticLayer
      .model("invoices")
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
        type: "string",
        aggregateWith: "sum",
        sql: ({ model }) => model.column("Total"),
      });

    const invoiceLinesModel = semanticLayer
      .model("invoice_lines")
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
        type: "string",
        aggregateWith: "sum",
        sql: ({ model }) => model.column("Quantity"),
      })
      .withMetric("total_unit_price", {
        type: "string",
        aggregateWith: "sum",
        sql: ({ model }) => model.column("UnitPrice"),
      });

    const tracksModel = semanticLayer
      .model("tracks")
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
      });

    const albumsModel = semanticLayer
      .model("albums")
      .fromTable("Album")
      .withDimension("album_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model }) => model.column("AlbumId"),
      })
      .withDimension("title", {
        type: "string",
        sql: ({ model }) => model.column("Title"),
      });

    const repository = semanticLayer
      .repository()
      .withModel(customersModel)
      .withModel(invoicesModel)
      .withModel(invoiceLinesModel)
      .withModel(tracksModel)
      .withModel(albumsModel)
      .joinOneToMany(
        "customers",
        "invoices",
        ({ sql, dimensions }) =>
          sql`${dimensions.customers.customer_id} = ${dimensions.invoices.customer_id}`,
      )
      .joinOneToMany(
        "invoices",
        "invoice_lines",
        ({ sql, dimensions }) =>
          sql`${dimensions.invoices.invoice_id} = ${dimensions.invoice_lines.invoice_id}`,
      )
      .joinOneToMany(
        "invoice_lines",
        "tracks",
        ({ sql, dimensions }) =>
          sql`${dimensions.invoice_lines.track_id} = ${dimensions.tracks.track_id}`,
      )
      .joinManyToMany(
        "tracks",
        "albums",
        ({ sql, dimensions }) =>
          sql`${dimensions.tracks.album_id} = ${dimensions.albums.album_id}`,
      );

    const queryBuilder = repository.build("postgresql");

    await it("can query one dimension and one metric", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: ["customers.customer_id"],
        metrics: ["invoices.total"],
        order: { "customers.customer_id": "asc" },
        limit: 10,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        { customers___customer_id: 1, invoices___total: "39.62" },
        { customers___customer_id: 2, invoices___total: "37.62" },
        { customers___customer_id: 3, invoices___total: "39.62" },
        { customers___customer_id: 4, invoices___total: "39.62" },
        { customers___customer_id: 5, invoices___total: "40.62" },
        { customers___customer_id: 6, invoices___total: "49.62" },
        { customers___customer_id: 7, invoices___total: "42.62" },
        { customers___customer_id: 8, invoices___total: "37.62" },
        { customers___customer_id: 9, invoices___total: "37.62" },
        { customers___customer_id: 10, invoices___total: "37.62" },
      ]);
    });

    await it("can query one dimension and multiple metrics", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: ["customers.customer_id"],
        metrics: ["invoices.total", "invoice_lines.total_unit_price"],
        order: { "customers.customer_id": "asc" },
        limit: 10,
      });

      const result = await client.query<
        InferSqlQueryResultType<typeof query, { "invoices.total": number }>
      >(query.sql, query.bindings);

      assert.deepEqual(result.rows, [
        {
          customers___customer_id: 1,
          invoices___total: "39.62",
          invoice_lines___total_unit_price: "39.62",
        },
        {
          customers___customer_id: 2,
          invoices___total: "37.62",
          invoice_lines___total_unit_price: "37.62",
        },
        {
          customers___customer_id: 3,
          invoices___total: "39.62",
          invoice_lines___total_unit_price: "39.62",
        },
        {
          customers___customer_id: 4,
          invoices___total: "39.62",
          invoice_lines___total_unit_price: "39.62",
        },
        {
          customers___customer_id: 5,
          invoices___total: "40.62",
          invoice_lines___total_unit_price: "40.62",
        },
        {
          customers___customer_id: 6,
          invoices___total: "49.62",
          invoice_lines___total_unit_price: "49.62",
        },
        {
          customers___customer_id: 7,
          invoices___total: "42.62",
          invoice_lines___total_unit_price: "42.62",
        },
        {
          customers___customer_id: 8,
          invoices___total: "37.62",
          invoice_lines___total_unit_price: "37.62",
        },
        {
          customers___customer_id: 9,
          invoices___total: "37.62",
          invoice_lines___total_unit_price: "37.62",
        },
        {
          customers___customer_id: 10,
          invoices___total: "37.62",
          invoice_lines___total_unit_price: "37.62",
        },
      ]);
    });

    await it("can query one dimension and metric and filter by a different metric", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: ["customers.customer_id"],
        metrics: ["invoices.total"],
        order: { "customers.customer_id": "asc" },
        limit: 10,
        filters: [
          {
            operator: "lt",
            member: "invoice_lines.total_unit_price",
            value: [38],
          },
        ],
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        { customers___customer_id: 2, invoices___total: "37.62" },
        { customers___customer_id: 8, invoices___total: "37.62" },
        { customers___customer_id: 9, invoices___total: "37.62" },
        { customers___customer_id: 10, invoices___total: "37.62" },
        { customers___customer_id: 11, invoices___total: "37.62" },
        { customers___customer_id: 12, invoices___total: "37.62" },
        { customers___customer_id: 13, invoices___total: "37.62" },
        { customers___customer_id: 14, invoices___total: "37.62" },
        { customers___customer_id: 16, invoices___total: "37.62" },
        { customers___customer_id: 18, invoices___total: "37.62" },
      ]);
    });

    await it("can query a metric and filter by a dimension", async () => {
      const query = queryBuilder.buildQuery({
        metrics: ["invoices.total"],
        filters: [
          { operator: "equals", member: "customers.customer_id", value: [1] },
        ],
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [{ invoices___total: "39.62" }]);
    });

    await it("can query multiple metrics and filter by a dimension", async () => {
      const query = queryBuilder.buildQuery({
        metrics: ["invoices.total", "invoice_lines.quantity"],
        filters: [
          { operator: "equals", member: "customers.customer_id", value: [1] },
        ],
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        { invoices___total: "39.62", invoice_lines___quantity: "38" },
      ]);
    });

    await it("can query dimensions only", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: ["customers.customer_id", "albums.title"],
        filters: [
          { operator: "equals", member: "customers.customer_id", value: [1] },
        ],
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        { customers___customer_id: 1, albums___title: "Ac�stico MTV" },
        { customers___customer_id: 1, albums___title: "Ac�stico MTV [Live]" },
        { customers___customer_id: 1, albums___title: "Afrociberdelia" },
        {
          customers___customer_id: 1,
          albums___title: "Appetite for Destruction",
        },
        { customers___customer_id: 1, albums___title: "Arquivo II" },
        {
          customers___customer_id: 1,
          albums___title: "Arquivo Os Paralamas Do Sucesso",
        },
        { customers___customer_id: 1, albums___title: "Ax� Bahia 2001" },
        {
          customers___customer_id: 1,
          albums___title: "BBC Sessions [Disc 1] [Live]",
        },
        {
          customers___customer_id: 1,
          albums___title: "Battlestar Galactica (Classic), Season 1",
        },
        { customers___customer_id: 1, albums___title: "Bongo Fury" },
        { customers___customer_id: 1, albums___title: "Carnaval 2001" },
        {
          customers___customer_id: 1,
          albums___title: "Chill: Brazil (Disc 1)",
        },
        { customers___customer_id: 1, albums___title: "Cidade Negra - Hits" },
        { customers___customer_id: 1, albums___title: "Da Lama Ao Caos" },
        { customers___customer_id: 1, albums___title: "Greatest Kiss" },
        { customers___customer_id: 1, albums___title: "Na Pista" },
        {
          customers___customer_id: 1,
          albums___title: "No More Tears (Remastered)",
        },
        { customers___customer_id: 1, albums___title: "Rattle And Hum" },
        { customers___customer_id: 1, albums___title: "Sibelius: Finlandia" },
        {
          customers___customer_id: 1,
          albums___title: "The World of Classical Favourites",
        },
        { customers___customer_id: 1, albums___title: "Tribute" },
        { customers___customer_id: 1, albums___title: "Use Your Illusion I" },
      ]);
    });
  });

  await describe("models from sql queries", async () => {
    const customersModel = semanticLayer
      .model("customers")
      .fromSqlQuery('select * from "Customer"')
      .withDimension("customer_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model, sql }) => sql`${model.column("CustomerId")}`,
      });

    const invoicesModel = semanticLayer
      .model("invoices")
      .fromSqlQuery('select * from "Invoice"')
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
        type: "string",
        aggregateWith: "sum",
        sql: ({ model }) => model.column("Total"),
      });

    const repository = semanticLayer
      .repository()
      .withModel(customersModel)
      .withModel(invoicesModel)
      .joinOneToMany(
        "customers",
        "invoices",
        ({ sql, dimensions }) =>
          sql`${dimensions.customers.customer_id} = ${dimensions.invoices.customer_id}`,
      );

    const queryBuilder = repository.build("postgresql");

    await it("can query one dimension and multiple metrics", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: ["customers.customer_id"],
        metrics: ["invoices.total"],
        order: { "customers.customer_id": "asc" },
        limit: 10,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          customers___customer_id: 1,
          invoices___total: "39.62",
        },
        {
          customers___customer_id: 2,
          invoices___total: "37.62",
        },
        {
          customers___customer_id: 3,
          invoices___total: "39.62",
        },
        {
          customers___customer_id: 4,
          invoices___total: "39.62",
        },
        {
          customers___customer_id: 5,
          invoices___total: "40.62",
        },
        {
          customers___customer_id: 6,
          invoices___total: "49.62",
        },
        {
          customers___customer_id: 7,
          invoices___total: "42.62",
        },
        {
          customers___customer_id: 8,
          invoices___total: "37.62",
        },
        {
          customers___customer_id: 9,
          invoices___total: "37.62",
        },
        {
          customers___customer_id: 10,
          invoices___total: "37.62",
        },
      ]);
    });
  });

  await describe("query schema", async () => {
    await it("can parse a valid query", () => {
      const customersModel = semanticLayer
        .model("customers")
        .fromSqlQuery('select * from "Customer"')
        .withDimension("customer_id", {
          type: "number",
          primaryKey: true,
          sql: ({ model, sql }) => sql`${model.column("CustomerId")}`,
        });

      const invoicesModel = semanticLayer
        .model("invoices")
        .fromSqlQuery('select * from "Invoice"')
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
          type: "string",
          aggregateWith: "sum",
          sql: ({ model }) => model.column("Total"),
        });

      const repository = semanticLayer
        .repository()
        .withModel(customersModel)
        .withModel(invoicesModel)
        .joinOneToMany(
          "customers",
          "invoices",
          ({ sql, dimensions }) =>
            sql`${dimensions.customers.customer_id} = ${dimensions.invoices.customer_id}`,
        );

      const queryBuilder = repository.build("postgresql");

      const query = {
        dimensions: ["customers.customer_id"],
        metrics: ["invoices.total"],
        order: { "customers.customer_id": "asc" },
        filters: [
          { operator: "equals", member: "customers.customer_id", value: [1] },
        ],
        limit: 10,
      };

      const parsed = queryBuilder.querySchema.safeParse(query);
      assert.ok(parsed.success);

      const jsonSchema = zodToJsonSchema(queryBuilder.querySchema);

      assert.deepEqual(jsonSchema, {
        type: "object",
        properties: {
          dimensions: {
            type: "array",
            items: {
              type: "string",
              enum: [
                "customers.customer_id",
                "invoices.invoice_id",
                "invoices.customer_id",
              ],
            },
          },
          metrics: {
            type: "array",
            items: {
              type: "string",
              enum: ["invoices.total"],
            },
          },
          filters: {
            type: "array",
            items: {
              anyOf: [
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "and",
                    },
                    filters: {
                      $ref: "#/properties/filters",
                    },
                  },
                  required: ["operator", "filters"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "or",
                    },
                    filters: {
                      $ref: "#/properties/filters",
                    },
                  },
                  required: ["operator", "filters"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "equals",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      type: "array",
                      items: {
                        anyOf: [
                          {
                            type: "string",
                          },
                          {
                            type: "number",
                          },
                          {
                            type: "integer",
                            format: "int64",
                          },
                          {
                            type: "boolean",
                          },
                          {
                            type: "string",
                            format: "date-time",
                          },
                        ],
                      },
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "notEquals",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      type: "array",
                      items: {
                        anyOf: [
                          {
                            type: "string",
                          },
                          {
                            type: "number",
                          },
                          {
                            type: "integer",
                            format: "int64",
                          },
                          {
                            type: "boolean",
                          },
                          {
                            type: "string",
                            format: "date-time",
                          },
                        ],
                      },
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "notSet",
                    },
                    member: {
                      type: "string",
                    },
                  },
                  required: ["operator", "member"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "set",
                    },
                    member: {
                      type: "string",
                    },
                  },
                  required: ["operator", "member"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "contains",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      type: "array",
                      items: {
                        type: "string",
                      },
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "notContains",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      type: "array",
                      items: {
                        type: "string",
                      },
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "startsWith",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      type: "array",
                      items: {
                        type: "string",
                      },
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "notStartsWith",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      type: "array",
                      items: {
                        type: "string",
                      },
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "endsWith",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      type: "array",
                      items: {
                        type: "string",
                      },
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "notEndsWith",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      type: "array",
                      items: {
                        type: "string",
                      },
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "gt",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      type: "array",
                      items: {
                        type: "number",
                      },
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "gte",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      type: "array",
                      items: {
                        type: "number",
                      },
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "lt",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      type: "array",
                      items: {
                        type: "number",
                      },
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "lte",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      type: "array",
                      items: {
                        type: "number",
                      },
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "inDateRange",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      anyOf: [
                        {
                          type: "string",
                        },
                        {
                          type: "object",
                          properties: {
                            startDate: {
                              anyOf: [
                                {
                                  type: "string",
                                },
                                {
                                  type: "string",
                                  format: "date-time",
                                },
                              ],
                            },
                            endDate: {
                              anyOf: [
                                {
                                  type: "string",
                                },
                                {
                                  type: "string",
                                  format: "date-time",
                                },
                              ],
                            },
                          },
                          required: ["startDate", "endDate"],
                          additionalProperties: false,
                        },
                      ],
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "notInDateRange",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      $ref: "#/properties/filters/items/anyOf/16/properties/value",
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "beforeDate",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      anyOf: [
                        {
                          type: "string",
                        },
                        {
                          type: "string",
                          format: "date-time",
                        },
                      ],
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "afterDate",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      $ref: "#/properties/filters/items/anyOf/18/properties/value",
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
              ],
            },
          },
          limit: {
            type: "number",
          },
          offset: {
            type: "number",
          },
          order: {
            type: "object",
            additionalProperties: {
              type: "string",
              enum: ["asc", "desc"],
            },
          },
        },
        additionalProperties: false,
        $schema: "http://json-schema.org/draft-07/schema#",
      });
    });
  });

  await describe("model descriptions and query introspection", async () => {
    const customersModel = semanticLayer
      .model("customers")
      .fromSqlQuery('select * from "Customer"')
      .withDimension("customer_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model, sql }) => sql`${model.column("CustomerId")}`,
        description: "The unique identifier of the customer",
      });

    const invoicesModel = semanticLayer
      .model("invoices")
      .fromSqlQuery('select * from "Invoice"')
      .withDimension("invoice_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model }) => model.column("InvoiceId"),
        description: "The unique identifier of the invoice",
      })
      .withDimension("customer_id", {
        type: "number",
        sql: ({ model }) => model.column("CustomerId"),
        description: "The unique identifier of the invoice customer",
      })
      .withMetric("total", {
        type: "string",
        aggregateWith: "sum",
        format: "percentage",
        sql: ({ model }) => model.column("Total"),
      });

    const repository = semanticLayer
      .repository()
      .withModel(customersModel)
      .withModel(invoicesModel)
      .joinOneToMany(
        "customers",
        "invoices",
        ({ sql, dimensions }) =>
          sql`${dimensions.customers.customer_id} = ${dimensions.invoices.customer_id}`,
      );

    const queryBuilder = repository.build("postgresql");

    await it("allows access to the model descriptions", () => {
      const docs: string[] = [];
      const dimensions = repository.getDimensions();
      const metrics = repository.getMetrics();
      for (const dimension of dimensions) {
        docs.push(
          `DIMENSION: ${dimension.getPath()}, TYPE: ${dimension.getType()}, DESCRIPTION: ${
            dimension.getDescription() ?? "-"
          }, FORMAT: ${dimension.getFormat() ?? "-"}`,
        );
      }
      for (const metric of metrics) {
        docs.push(
          `METRIC: ${metric.getPath()}, TYPE: ${metric.getType()}, DESCRIPTION: ${
            metric.getDescription() ?? "-"
          }, FORMAT: ${metric.getFormat() ?? "-"}`,
        );
      }

      assert.deepEqual(docs, [
        "DIMENSION: customers.customer_id, TYPE: number, DESCRIPTION: The unique identifier of the customer, FORMAT: -",
        "DIMENSION: invoices.invoice_id, TYPE: number, DESCRIPTION: The unique identifier of the invoice, FORMAT: -",
        "DIMENSION: invoices.customer_id, TYPE: number, DESCRIPTION: The unique identifier of the invoice customer, FORMAT: -",
        "METRIC: invoices.total, TYPE: string, DESCRIPTION: -, FORMAT: percentage",
      ]);
    });

    await it("allows introspection of a query", () => {
      const query: QueryBuilderQuery<typeof queryBuilder> = {
        dimensions: [
          "customers.customer_id",
          "invoices.invoice_id",
          "invoices.customer_id",
        ],
        metrics: ["invoices.total"],
      };

      const introspection = queryBuilder.introspect(query);

      assert.deepEqual(introspection, {
        customers___customer_id: {
          memberType: "dimension",
          path: "customers.customer_id",
          type: "number",
          description: "The unique identifier of the customer",
          format: undefined,
        },
        invoices___invoice_id: {
          memberType: "dimension",
          path: "invoices.invoice_id",
          type: "number",
          description: "The unique identifier of the invoice",
          format: undefined,
        },
        invoices___customer_id: {
          memberType: "dimension",
          path: "invoices.customer_id",
          type: "number",
          description: "The unique identifier of the invoice customer",
          format: undefined,
        },
        invoices___total: {
          memberType: "metric",
          path: "invoices.total",
          format: "percentage",
          type: "string",
          description: undefined,
        },
      });
    });
  });

  await describe("fill repository", async () => {
    const customersModel = semanticLayer
      .model("customers")
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
            "last_name",
          )}`,
      })
      .withDimension("company", {
        type: "string",
        sql: ({ model }) => model.column("Company"),
      })
      .withDimension("address", {
        type: "string",
        sql: ({ model }) => model.column("Address"),
      })
      .withDimension("city", {
        type: "string",
        sql: ({ model }) => model.column("City"),
      })
      .withDimension("state", {
        type: "string",
        sql: ({ model }) => model.column("State"),
      })
      .withDimension("country", {
        type: "string",
        sql: ({ model }) => model.column("Country"),
      })
      .withDimension("postal_code", {
        type: "string",
        sql: ({ model }) => model.column("PostalCode"),
      })
      .withDimension("phone", {
        type: "string",
        sql: ({ model }) => model.column("Phone"),
      })
      .withDimension("fax", {
        type: "string",
        sql: ({ model }) => model.column("Fax"),
      })
      .withDimension("email", {
        type: "string",
        sql: ({ model }) => model.column("Email"),
      });

    const invoicesModel = semanticLayer
      .model("invoices")
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
      .withDimension("billing_address", {
        type: "string",
        sql: ({ model }) => model.column("BillingAddress"),
      })
      .withDimension("billing_city", {
        type: "string",
        sql: ({ model }) => model.column("BillingCity"),
      })
      .withDimension("billing_state", {
        type: "string",
        sql: ({ model }) => model.column("BillingState"),
      })
      .withDimension("billing_country", {
        type: "string",
        sql: ({ model }) => model.column("BillingCountry"),
      })
      .withDimension("billing_postal_code", {
        type: "string",
        sql: ({ model }) => model.column("BillingPostalCode"),
      })
      .withDimension("total", {
        type: "string",
        sql: ({ model }) => model.column("Total"),
      })
      .withMetric("sum_total", {
        type: "number",
        aggregateWith: "sum",
        description: "Sum of the invoice totals across dimensions.",
        sql: ({ model }) => model.dimension("total"),
      });

    const invoiceLinesModel = semanticLayer
      .model("invoice_lines")
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
      .withDimension("unit_price", {
        type: "string",
        sql: ({ model }) => model.column("UnitPrice"),
      })
      .withDimension("quantity", {
        type: "string",
        sql: ({ model }) => model.column("Quantity"),
      })
      .withMetric("sum_quantity", {
        type: "number",
        aggregateWith: "sum",
        description: "Sum of the track quantities across dimensions.",
        sql: ({ model }) => model.dimension("quantity"),
      })
      .withMetric("sum_unit_price", {
        type: "number",
        aggregateWith: "sum",
        description: "Sum of the track unit prices across dimensions.",
        sql: ({ model }) => model.dimension("unit_price"),
      });

    const tracksModel = semanticLayer
      .model("tracks")
      .fromTable("Track")
      .withDimension("track_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model }) => model.column("TrackId"),
      })
      .withDimension("album_id", {
        type: "number",
        sql: ({ model }) => model.column("AlbumId"),
      })
      .withDimension("media_type_id", {
        type: "number",
        sql: ({ model }) => model.column("MediaTypeId"),
      })
      .withDimension("genre_id", {
        type: "number",
        sql: ({ model }) => model.column("GenreId"),
      })
      .withDimension("name", {
        type: "string",
        sql: ({ model }) => model.column("Name"),
      })
      .withDimension("composer", {
        type: "string",
        sql: ({ model }) => model.column("Composer"),
      })
      .withDimension("milliseconds", {
        type: "number",
        sql: ({ model }) => model.column("Milliseconds"),
      })
      .withDimension("bytes", {
        type: "number",
        sql: ({ model }) => model.column("Bytes"),
      })
      .withDimension("unit_price", {
        type: "string",
        sql: ({ model }) => model.column("UnitPrice"),
      })
      .withMetric("sum_unit_price", {
        type: "number",
        aggregateWith: "sum",
        description: "Sum of the track unit prices across dimensions.",
        sql: ({ model }) => model.dimension("unit_price"),
      });

    const albumsModel = semanticLayer
      .model("albums")
      .fromTable("Album")
      .withDimension("album_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model }) => model.column("AlbumId"),
      })
      .withDimension("artist_id", {
        type: "number",
        sql: ({ model }) => model.column("ArtistId"),
      })
      .withDimension("title", {
        type: "string",
        sql: ({ model }) => model.column("Title"),
      });

    const artistModel = semanticLayer
      .model("artists")
      .fromTable("Artist")
      .withDimension("artist_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model }) => model.column("ArtistId"),
      })
      .withDimension("name", {
        type: "string",
        sql: ({ model }) => model.column("Name"),
      });

    const mediaTypeModel = semanticLayer
      .model("media_types")
      .fromTable("MediaType")
      .withDimension("media_type_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model }) => model.column("MediaTypeId"),
      })
      .withDimension("name", {
        type: "string",
        sql: ({ model }) => model.column("Name"),
      });

    const genreModel = semanticLayer
      .model("genres")
      .fromTable("Genre")
      .withDimension("name", {
        type: "string",
        sql: ({ model }) => model.column("Name"),
      })
      .withDimension("genre_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model }) => model.column("GenreId"),
      });

    const playlistModel = semanticLayer
      .model("playlists")
      .fromTable("Playlist")
      .withDimension("playlist_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model }) => model.column("PlaylistId"),
      })
      .withDimension("name", {
        type: "string",
        sql: ({ model }) => model.column("Name"),
      });

    const playlistTrackModel = semanticLayer
      .model("playlist_tracks")
      .fromTable("PlaylistTrack")
      .withDimension("playlist_id", {
        type: "number",
        sql: ({ model }) => model.column("PlaylistId"),
      })
      .withDimension("track_id", {
        type: "number",
        sql: ({ model }) => model.column("TrackId"),
      });

    const repository = semanticLayer
      .repository()
      .withModel(customersModel)
      .withModel(invoicesModel)
      .withModel(invoiceLinesModel)
      .withModel(tracksModel)
      .withModel(albumsModel)
      .withModel(artistModel)
      .withModel(mediaTypeModel)
      .withModel(genreModel)
      .withModel(playlistModel)
      .withModel(playlistTrackModel)
      .joinOneToMany(
        "customers",
        "invoices",
        ({ sql, dimensions }) =>
          sql`${dimensions.customers.customer_id} = ${dimensions.invoices.customer_id}`,
      )
      .joinOneToMany(
        "invoices",
        "invoice_lines",
        ({ sql, dimensions }) =>
          sql`${dimensions.invoices.invoice_id} = ${dimensions.invoice_lines.invoice_id}`,
      )
      .joinManyToOne(
        "invoice_lines",
        "tracks",
        ({ sql, dimensions }) =>
          sql`${dimensions.invoice_lines.track_id} = ${dimensions.tracks.track_id}`,
      )
      .joinOneToMany(
        "albums",
        "tracks",
        ({ sql, dimensions }) =>
          sql`${dimensions.tracks.album_id} = ${dimensions.albums.album_id}`,
      )
      .joinManyToOne(
        "albums",
        "artists",
        ({ sql, dimensions }) =>
          sql`${dimensions.albums.artist_id} = ${dimensions.artists.artist_id}`,
      )
      .joinOneToOne(
        "tracks",
        "media_types",
        ({ sql, dimensions }) =>
          sql`${dimensions.tracks.media_type_id} = ${dimensions.media_types.media_type_id}`,
      )
      .joinOneToOne(
        "tracks",
        "genres",
        ({ sql, dimensions }) =>
          sql`${dimensions.tracks.genre_id} = ${dimensions.genres.genre_id}`,
      )
      .joinManyToMany(
        "playlists",
        "playlist_tracks",
        ({ sql, dimensions }) =>
          sql`${dimensions.playlists.playlist_id} = ${dimensions.playlist_tracks.playlist_id}`,
      )
      .joinManyToMany(
        "playlist_tracks",
        "tracks",
        ({ sql, dimensions }) =>
          sql`${dimensions.playlist_tracks.track_id} = ${dimensions.tracks.track_id}`,
      );

    const queryBuilder = repository.build("postgresql");

    await it("should return distinct results for dimension only query", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: ["artists.name"],
        filters: [
          {
            operator: "equals",
            member: "genres.name",
            value: ["Rock"],
          },
        ],
        order: { "artists.name": "asc" },
        limit: 10,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          artists___name: "AC/DC",
        },
        {
          artists___name: "Accept",
        },
        {
          artists___name: "Aerosmith",
        },
        {
          artists___name: "Alanis Morissette",
        },
        {
          artists___name: "Alice In Chains",
        },
        {
          artists___name: "Audioslave",
        },
        {
          artists___name: "Creedence Clearwater Revival",
        },
        {
          artists___name: "David Coverdale",
        },
        {
          artists___name: "Deep Purple",
        },
        {
          artists___name: "Def Leppard",
        },
      ]);
    });
  });
});
