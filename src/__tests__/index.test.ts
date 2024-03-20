import * as C from "../index.js";
import * as assert from "node:assert/strict";

import {
  PostgreSqlContainer,
  StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { after, before, describe, it } from "node:test";

import { InferSqlQueryResultType } from "../index.js";
import fs from "node:fs/promises";
import path from "node:path";
import pg from "pg";

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
    const customersModel = C.model("customers")
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

    const invoicesModel = C.model("invoices")
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

    const invoiceLinesModel = C.model("invoice_lines")
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

    const tracksModel = C.model("tracks")
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

    const albumsModel = C.model("albums")
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

    const repository = C.repository()
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
      const query = queryBuilder.build({
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
      const query = queryBuilder.build({
        dimensions: ["customers.customer_id"],
        metrics: ["invoices.total", "invoice_lines.total_unit_price"],
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
      const query = queryBuilder.build({
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
      const query = queryBuilder.build({
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
      const query = queryBuilder.build({
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
      const query = queryBuilder.build({
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
    const customersModel = C.model("customers")
      .fromSqlQuery('select * from "Customer"')
      .withDimension("customer_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model, sql }) => sql`${model.column("CustomerId")}`,
      });

    const invoicesModel = C.model("invoices")
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

    const repository = C.repository()
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
      const query = queryBuilder.build({
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
});
