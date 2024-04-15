import * as assert from "node:assert/strict";
import * as semanticLayer from "../index.js";

import { after, before, describe, it } from "node:test";
import {
  PostgreSqlContainer,
  StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { InferSqlQueryResultType, QueryBuilderQuery } from "../index.js";

import fs from "node:fs/promises";
import path from "node:path";
import pg from "pg";
import { zodToJsonSchema } from "zod-to-json-schema";

// import { format as sqlFormat } from "sql-formatter";

const __dirname = path.dirname(new URL(import.meta.url).pathname);

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
      options: "-c TimeZone=UTC",
    });

    await client.connect();
    await client.query(bootstrapSql);

    const timezoneResult = await client.query("SHOW TIMEZONE");
    const timezone = timezoneResult.rows[0].TimeZone;

    assert.equal(timezone, "UTC");
  });

  after(async () => {
    await client.end();
    await container.stop();
  });

  await describe("models from tables", async () => {
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
      .withDimension("company", {
        type: "string",
        sql: ({ model }) => model.column("Company"),
      })
      .withDimension("country", {
        type: "string",
        sql: ({ model }) => model.column("Country"),
      })
      .withDimension("full_name", {
        type: "string",
        sql: ({ model, sql }) =>
          sql`${model.dimension("first_name")} || ' ' || ${model.dimension(
            "last_name",
          )}`,
      })
      .withMetric("count", {
        type: "string",
        aggregateWith: "count",
        sql: ({ model }) => model.column("CustomerId"),
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
      .withDimension("invoice_date", {
        type: "datetime",
        sql: ({ model }) => model.column("InvoiceDate"),
      })
      .withMetric("total", {
        type: "string",
        aggregateWith: "sum",
        sql: ({ model }) => model.column("Total"),
      });

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
      });

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
        ({ sql, models }) =>
          sql`${models.customers.dimension(
            "customer_id",
          )} = ${models.invoices.dimension("customer_id")}`,
      )
      .joinOneToMany(
        "invoices",
        "invoice_lines",
        ({ sql, models, getContext }) =>
          sql`${models.invoices.dimension(
            "invoice_id",
          )} = ${models.invoice_lines.dimension("invoice_id")} ${getContext()}`,
      )
      .joinOneToMany(
        "invoice_lines",
        "tracks",
        ({ sql, models }) =>
          sql`${models.invoice_lines.dimension(
            "track_id",
          )} = ${models.tracks.dimension("track_id")}`,
      )
      .joinManyToMany(
        "tracks",
        "albums",
        ({ sql, models }) =>
          sql`${models.tracks.dimension(
            "album_id",
          )} = ${models.albums.dimension("album_id")}`,
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

    await it("can query a metric and slice it correctly by a non primary key dimension", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: ["customers.country"],
        metrics: ["customers.count"],
        order: {
          "customers.country": "asc",
        },
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        { customers___country: "Argentina", customers___count: "1" },
        { customers___country: "Australia", customers___count: "1" },
        { customers___country: "Austria", customers___count: "1" },
        { customers___country: "Belgium", customers___count: "1" },
        { customers___country: "Brazil", customers___count: "5" },
        { customers___country: "Canada", customers___count: "8" },
        { customers___country: "Chile", customers___count: "1" },
        { customers___country: "Czech Republic", customers___count: "2" },
        { customers___country: "Denmark", customers___count: "1" },
        { customers___country: "Finland", customers___count: "1" },
        { customers___country: "France", customers___count: "5" },
        { customers___country: "Germany", customers___count: "4" },
        { customers___country: "Hungary", customers___count: "1" },
        { customers___country: "India", customers___count: "2" },
        { customers___country: "Ireland", customers___count: "1" },
        { customers___country: "Italy", customers___count: "1" },
        { customers___country: "Netherlands", customers___count: "1" },
        { customers___country: "Norway", customers___count: "1" },
        { customers___country: "Poland", customers___count: "1" },
        { customers___country: "Portugal", customers___count: "2" },
        { customers___country: "Spain", customers___count: "1" },
        { customers___country: "Sweden", customers___count: "1" },
        { customers___country: "USA", customers___count: "13" },
        { customers___country: "United Kingdom", customers___count: "3" },
      ]);
    });

    await it("will correctly load distinct dimensions when no metrics are loaded", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: ["customers.country"],
        order: { "customers.country": "asc" },
        limit: 10,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        { customers___country: "Argentina" },
        { customers___country: "Australia" },
        { customers___country: "Austria" },
        { customers___country: "Belgium" },
        { customers___country: "Brazil" },
        { customers___country: "Canada" },
        { customers___country: "Chile" },
        { customers___country: "Czech Republic" },
        { customers___country: "Denmark" },
        { customers___country: "Finland" },
      ]);
    });

    await it("will remove non projected members from order clause", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: [
          "customers.customer_id",
          "customers.full_name",
          "invoice_lines.invoice_id",
        ],
        metrics: [],
        limit: 10,
        order: { "invoices.invoice_date": "asc" },
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          customers___customer_id: 1,
          customers___full_name: "Lu�s Gon�alves",
          invoice_lines___invoice_id: 121,
        },
        {
          customers___customer_id: 1,
          customers___full_name: "Lu�s Gon�alves",
          invoice_lines___invoice_id: 316,
        },
        {
          customers___customer_id: 1,
          customers___full_name: "Lu�s Gon�alves",
          invoice_lines___invoice_id: 143,
        },
        {
          customers___customer_id: 1,
          customers___full_name: "Lu�s Gon�alves",
          invoice_lines___invoice_id: 195,
        },
        {
          customers___customer_id: 1,
          customers___full_name: "Lu�s Gon�alves",
          invoice_lines___invoice_id: 327,
        },
        {
          customers___customer_id: 1,
          customers___full_name: "Lu�s Gon�alves",
          invoice_lines___invoice_id: 98,
        },
        {
          customers___customer_id: 1,
          customers___full_name: "Lu�s Gon�alves",
          invoice_lines___invoice_id: 382,
        },
        {
          customers___customer_id: 2,
          customers___full_name: "Leonie K�hler",
          invoice_lines___invoice_id: 1,
        },
        {
          customers___customer_id: 2,
          customers___full_name: "Leonie K�hler",
          invoice_lines___invoice_id: 219,
        },
        {
          customers___customer_id: 2,
          customers___full_name: "Leonie K�hler",
          invoice_lines___invoice_id: 67,
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

    await it("can correctly query datetime granularities", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: [
          "invoices.invoice_id",
          "invoices.invoice_date",
          "invoices.invoice_date.time",
          "invoices.invoice_date.date",
          "invoices.invoice_date.year",
          "invoices.invoice_date.quarter",
          "invoices.invoice_date.quarter_of_year",
          "invoices.invoice_date.month",
          "invoices.invoice_date.month_num",
          "invoices.invoice_date.week",
          "invoices.invoice_date.week_num",
          "invoices.invoice_date.day_of_month",
          "invoices.invoice_date.hour",
          "invoices.invoice_date.hour_of_day",
          "invoices.invoice_date.minute",
        ],
        filters: [
          { operator: "equals", member: "invoices.invoice_id", value: [6] },
        ],
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          invoices___invoice_date: new Date("2009-01-19T00:00:00.000+00:00"),
          invoices___invoice_date___date: new Date(
            "2009-01-19T00:00:00.000+00:00",
          ),
          invoices___invoice_date___day_of_month: 19,
          invoices___invoice_date___hour: "2009-01-19 00",
          invoices___invoice_date___hour_of_day: 0,
          invoices___invoice_date___minute: "2009-01-19 00:00",
          invoices___invoice_date___month: "2009-01",
          invoices___invoice_date___month_num: 1,
          invoices___invoice_date___quarter: "2009-Q1",
          invoices___invoice_date___quarter_of_year: 1,
          invoices___invoice_date___time: "00:00:00",
          invoices___invoice_date___week: "2009-W04",
          invoices___invoice_date___week_num: 4,
          invoices___invoice_date___year: 2009,
          invoices___invoice_id: 6,
        },
      ]);
    });

    await it("can introspect if dimension is a primary key", () => {
      assert.ok(
        repository.getDimension("customers.customer_id").isPrimaryKey(),
      );
    });

    await it("can introspect if dimension is a granularity", () => {
      assert.ok(
        repository
          .getDimension("invoices.invoice_date.day_of_month")
          .isGranularity(),
      );
    });

    await it("can query adhoc metrics", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: ["customers.customer_id"],
        metrics: [
          { aggregateWith: "count", dimension: "invoices.invoice_id" },
          "invoice_lines.total_unit_price",
        ],
        order: { "customers.customer_id": "asc" },
        limit: 5,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          customers___customer_id: 1,
          invoices___invoice_id___adhoc_count: "7",
          invoice_lines___total_unit_price: "39.62",
        },
        {
          customers___customer_id: 2,
          invoices___invoice_id___adhoc_count: "7",
          invoice_lines___total_unit_price: "37.62",
        },
        {
          customers___customer_id: 3,
          invoices___invoice_id___adhoc_count: "7",
          invoice_lines___total_unit_price: "39.62",
        },
        {
          customers___customer_id: 4,
          invoices___invoice_id___adhoc_count: "7",
          invoice_lines___total_unit_price: "39.62",
        },
        {
          customers___customer_id: 5,
          invoices___invoice_id___adhoc_count: "7",
          invoice_lines___total_unit_price: "40.62",
        },
      ]);
    });

    await it("can query adhoc metrics on date/time granularity column", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: ["customers.customer_id"],
        metrics: [
          { aggregateWith: "min", dimension: "invoices.invoice_date.quarter" },
          { aggregateWith: "min", dimension: "invoices.invoice_date" },
        ],
        order: { "customers.customer_id": "asc" },
        limit: 5,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          customers___customer_id: 1,
          invoices___invoice_date___quarter___adhoc_min: "2010-Q1",
          invoices___invoice_date___adhoc_min: new Date(
            "2010-03-11T00:00:00.000Z",
          ),
        },
        {
          customers___customer_id: 2,
          invoices___invoice_date___quarter___adhoc_min: "2009-Q1",
          invoices___invoice_date___adhoc_min: new Date(
            "2009-01-01T00:00:00.000Z",
          ),
        },
        {
          customers___customer_id: 3,
          invoices___invoice_date___quarter___adhoc_min: "2010-Q1",
          invoices___invoice_date___adhoc_min: new Date(
            "2010-03-11T00:00:00.000Z",
          ),
        },
        {
          customers___customer_id: 4,
          invoices___invoice_date___quarter___adhoc_min: "2009-Q1",
          invoices___invoice_date___adhoc_min: new Date(
            "2009-01-02T00:00:00.000Z",
          ),
        },
        {
          customers___customer_id: 5,
          invoices___invoice_date___quarter___adhoc_min: "2009-Q4",
          invoices___invoice_date___adhoc_min: new Date(
            "2009-12-08T00:00:00.000Z",
          ),
        },
      ]);
    });
  });

  await describe("models from sql queries", async () => {
    const customersModel = semanticLayer
      .model()
      .withName("customers")
      .fromSqlQuery(
        ({ sql, identifier }) => sql`select * from ${identifier("Customer")}`,
      )
      .withDimension("customer_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model, sql }) => sql`${model.column("CustomerId")}`,
      });

    const invoicesModel = semanticLayer
      .model()
      .withName("invoices")
      .fromSqlQuery(
        ({ sql, identifier }) => sql`select * from ${identifier("Invoice")}`,
      )
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
        ({ sql, models }) =>
          sql`${models.customers.dimension(
            "customer_id",
          )} = ${models.invoices.dimension("customer_id")}`,
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
        .model()
        .withName("customers")
        .fromSqlQuery(
          ({ sql, identifier }) => sql`select * from ${identifier("Customer")}`,
        )
        .withDimension("customer_id", {
          type: "number",
          primaryKey: true,
          sql: ({ model, sql }) => sql`${model.column("CustomerId")}`,
        });

      const invoicesModel = semanticLayer
        .model()
        .withName("invoices")
        .fromSqlQuery(
          ({ sql, identifier }) => sql`select * from ${identifier("Invoice")}`,
        )
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
          ({ sql, models }) =>
            sql`${models.customers.dimension(
              "customer_id",
            )} = ${models.invoices.dimension("customer_id")}`,
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
          dimensions: { type: "array", items: { type: "string" } },
          metrics: {
            type: "array",
            items: {
              anyOf: [
                { type: "string" },
                {
                  type: "object",
                  properties: {
                    aggregateWith: {
                      type: "string",
                      enum: ["sum", "count", "min", "max", "avg"],
                    },
                    dimension: { type: "string" },
                  },
                  required: ["aggregateWith", "dimension"],
                  additionalProperties: false,
                },
              ],
            },
          },
          filters: {
            type: "array",
            items: {
              anyOf: [
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "and" },
                    filters: { $ref: "#/properties/filters" },
                  },
                  required: ["operator", "filters"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "or" },
                    filters: { $ref: "#/properties/filters" },
                  },
                  required: ["operator", "filters"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "equals" },
                    member: { type: "string" },
                    value: {
                      type: "array",
                      items: {
                        anyOf: [
                          { type: "string" },
                          { type: "number" },
                          { type: "integer", format: "int64" },
                          { type: "boolean" },
                          { type: "string", format: "date-time" },
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
                    operator: { type: "string", const: "notEquals" },
                    member: { type: "string" },
                    value: {
                      type: "array",
                      items: {
                        anyOf: [
                          { type: "string" },
                          { type: "number" },
                          { type: "integer", format: "int64" },
                          { type: "boolean" },
                          { type: "string", format: "date-time" },
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
                    operator: { type: "string", const: "notSet" },
                    member: { type: "string" },
                  },
                  required: ["operator", "member"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "set" },
                    member: { type: "string" },
                  },
                  required: ["operator", "member"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "contains" },
                    member: { type: "string" },
                    value: { type: "array", items: { type: "string" } },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "notContains" },
                    member: { type: "string" },
                    value: { type: "array", items: { type: "string" } },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "startsWith" },
                    member: { type: "string" },
                    value: { type: "array", items: { type: "string" } },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "notStartsWith" },
                    member: { type: "string" },
                    value: { type: "array", items: { type: "string" } },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "endsWith" },
                    member: { type: "string" },
                    value: { type: "array", items: { type: "string" } },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "notEndsWith" },
                    member: { type: "string" },
                    value: { type: "array", items: { type: "string" } },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "gt" },
                    member: { type: "string" },
                    value: { type: "array", items: { type: "number" } },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "gte" },
                    member: { type: "string" },
                    value: { type: "array", items: { type: "number" } },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "lt" },
                    member: { type: "string" },
                    value: { type: "array", items: { type: "number" } },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "lte" },
                    member: { type: "string" },
                    value: { type: "array", items: { type: "number" } },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "inDateRange" },
                    member: { type: "string" },
                    value: {
                      anyOf: [
                        { type: "string" },
                        {
                          type: "object",
                          properties: {
                            startDate: {
                              anyOf: [
                                { type: "string" },
                                { type: "string", format: "date-time" },
                              ],
                            },
                            endDate: {
                              anyOf: [
                                { type: "string" },
                                { type: "string", format: "date-time" },
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
                    operator: { type: "string", const: "notInDateRange" },
                    member: { type: "string" },
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
                    operator: { type: "string", const: "beforeDate" },
                    member: { type: "string" },
                    value: {
                      anyOf: [
                        { type: "string" },
                        { type: "string", format: "date-time" },
                      ],
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                },
                {
                  type: "object",
                  properties: {
                    operator: { type: "string", const: "afterDate" },
                    member: { type: "string" },
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
          limit: { type: "number" },
          offset: { type: "number" },
          order: {
            type: "object",
            additionalProperties: { type: "string", enum: ["asc", "desc"] },
          },
        },
        additionalProperties: false,
        $schema: "http://json-schema.org/draft-07/schema#",
      });
    });
  });

  await describe("model descriptions and query introspection", async () => {
    const customersModel = semanticLayer
      .model()
      .withName("customers")
      .fromTable("Customer")
      .withDimension("customer_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model, sql }) => sql`${model.column("CustomerId")}`,
        description: "The unique identifier of the customer",
      });

    const invoicesModel = semanticLayer
      .model()
      .withName("invoices")
      .fromTable("Invoice")
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
        ({ sql, models }) =>
          sql`${models.customers.dimension(
            "customer_id",
          )} = ${models.invoices.dimension("customer_id")}`,
      );

    const queryBuilder = repository.build("postgresql");

    await it("allows access to the model descriptions", () => {
      const docs: string[] = [];
      const dimensions = repository.getDimensions();
      const metrics = repository.getMetrics();
      const joins = repository.getJoins();

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
      for (const join of joins) {
        docs.push(`JOIN: ${join.left} -> ${join.right}, TYPE: ${join.type}`);
      }

      assert.deepEqual(docs, [
        "DIMENSION: customers.customer_id, TYPE: number, DESCRIPTION: The unique identifier of the customer, FORMAT: -",
        "DIMENSION: invoices.invoice_id, TYPE: number, DESCRIPTION: The unique identifier of the invoice, FORMAT: -",
        "DIMENSION: invoices.customer_id, TYPE: number, DESCRIPTION: The unique identifier of the invoice customer, FORMAT: -",
        "METRIC: invoices.total, TYPE: string, DESCRIPTION: -, FORMAT: percentage",
        "JOIN: customers -> invoices, TYPE: oneToMany",
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
          isPrimaryKey: true,
          isGranularity: false,
        },
        invoices___invoice_id: {
          memberType: "dimension",
          path: "invoices.invoice_id",
          type: "number",
          description: "The unique identifier of the invoice",
          format: undefined,
          isPrimaryKey: true,
          isGranularity: false,
        },
        invoices___customer_id: {
          memberType: "dimension",
          path: "invoices.customer_id",
          type: "number",
          description: "The unique identifier of the invoice customer",
          format: undefined,
          isPrimaryKey: false,
          isGranularity: false,
        },
        invoices___total: {
          memberType: "metric",
          path: "invoices.total",
          format: "percentage",
          type: "string",
          description: undefined,
          isPrimaryKey: false,
          isGranularity: false,
        },
      });
    });
  });

  await describe("full repository", async () => {
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
        description: "Sum of the invoice totals across models.",
        sql: ({ model }) => model.dimension("total"),
      });

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
        description: "Sum of the track quantities across models.",
        sql: ({ model }) => model.dimension("quantity"),
      })
      .withMetric("sum_unit_price", {
        type: "number",
        aggregateWith: "sum",
        description: "Sum of the track unit prices across models.",
        sql: ({ model }) => model.dimension("unit_price"),
      });

    const tracksModel = semanticLayer
      .model()
      .withName("tracks")
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
        description: "Sum of the track unit prices across models.",
        sql: ({ model }) => model.dimension("unit_price"),
      });

    const albumsModel = semanticLayer
      .model()
      .withName("albums")
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
      });

    const mediaTypeModel = semanticLayer
      .model()
      .withName("media_types")
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
      .model()
      .withName("genres")
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
      .model()
      .withName("playlists")
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
      .model()
      .withName("playlist_tracks")
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
        ({ sql, models }) =>
          sql`${models.customers.dimension(
            "customer_id",
          )} = ${models.invoices.dimension("customer_id")}`,
      )
      .joinOneToMany(
        "invoices",
        "invoice_lines",
        ({ sql, models }) =>
          sql`${models.invoices.dimension(
            "invoice_id",
          )} = ${models.invoice_lines.dimension("invoice_id")}`,
      )
      .joinManyToOne(
        "invoice_lines",
        "tracks",
        ({ sql, models }) =>
          sql`${models.invoice_lines.dimension(
            "track_id",
          )} = ${models.tracks.dimension("track_id")}`,
      )
      .joinOneToMany(
        "albums",
        "tracks",
        ({ sql, models }) =>
          sql`${models.tracks.dimension(
            "album_id",
          )} = ${models.albums.dimension("album_id")}`,
      )
      .joinManyToOne(
        "albums",
        "artists",
        ({ sql, models }) =>
          sql`${models.albums.dimension(
            "artist_id",
          )} = ${models.artists.dimension("artist_id")}`,
      )
      .joinOneToOne(
        "tracks",
        "media_types",
        ({ sql, models }) =>
          sql`${models.tracks.dimension(
            "media_type_id",
          )} = ${models.media_types.dimension("media_type_id")}`,
      )
      .joinOneToOne(
        "tracks",
        "genres",
        ({ sql, models }) =>
          sql`${models.tracks.dimension(
            "genre_id",
          )} = ${models.genres.dimension("genre_id")}`,
      )
      .joinManyToMany(
        "playlists",
        "playlist_tracks",
        ({ sql, models }) =>
          sql`${models.playlists.dimension(
            "playlist_id",
          )} = ${models.playlist_tracks.dimension("playlist_id")}`,
      )
      .joinManyToMany(
        "playlist_tracks",
        "tracks",
        ({ sql, models }) =>
          sql`${models.playlist_tracks.dimension(
            "track_id",
          )} = ${models.tracks.dimension("track_id")}`,
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

    await it("should return order results by default", async () => {
      const query = queryBuilder.buildQuery({
        dimensions: ["artists.name"],
        filters: [
          {
            operator: "equals",
            member: "genres.name",
            value: ["Rock"],
          },
        ],
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

    await it("should return same query after it's parsed by schema", async () => {
      const query = {
        dimensions: ["artists.name"],
        metrics: ["tracks.sum_unit_price"],
        filters: [
          {
            operator: "equals",
            member: "genres.name",
            value: ["Rock"],
          },
        ],
        order: { "artists.name": "asc" },
        limit: 10,
      };

      const parsedQuery = queryBuilder.querySchema.parse(query);
      assert.deepEqual(query, parsedQuery);
    });
  });

  describe("repository with context", async () => {
    await it("propagates context to all sql functions", async () => {
      type QueryContext = {
        customerId: number;
      };

      const customersModel = semanticLayer
        .model<QueryContext>()
        .withName("customers")
        .fromSqlQuery(
          ({ sql, identifier, getContext }) =>
            sql`select * from ${identifier("Customer")} where ${identifier(
              "CustomerId",
            )} = ${getContext().customerId}`,
        )
        .withDimension("customer_id", {
          type: "number",
          primaryKey: true,
          sql: ({ model, sql, getContext }) =>
            sql`${model.column("CustomerId")} || cast(${
              getContext().customerId
            } as text)`,
        })
        .withDimension("first_name", {
          type: "string",
          sql: ({ model }) => model.column("FirstName"),
        });

      const invoicesModel = semanticLayer
        .model<QueryContext>()
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
        });

      const repository = semanticLayer
        .repository<QueryContext>()
        .withModel(customersModel)
        .withModel(invoicesModel)
        .joinOneToMany(
          "customers",
          "invoices",
          ({ sql, models, getContext }) =>
            sql`${models.customers.dimension(
              "customer_id",
            )} = ${models.invoices.dimension("customer_id")} and ${
              getContext().customerId
            } = ${getContext().customerId}`,
        );

      const queryBuilder = repository.build("postgresql");
      const query = queryBuilder.buildQuery(
        {
          dimensions: ["customers.customer_id", "invoices.invoice_id"],
        },
        { customerId: 1 },
      );

      assert.equal(
        query.sql,
        'select "q0"."customers___customer_id" as "customers___customer_id", "q0"."invoices___invoice_id" as "invoices___invoice_id" from (select "invoices_query"."customers___customer_id" as "customers___customer_id", "invoices_query"."invoices___invoice_id" as "invoices___invoice_id" from (select distinct "Invoice"."InvoiceId" as "invoices___invoice_id", "customers"."CustomerId" || cast($1 as text) as "customers___customer_id" from "Invoice" right join (select * from "Customer" where "CustomerId" = $2) as customers on "customers"."CustomerId" || cast($3 as text) = "Invoice"."CustomerId" and $4 = $5) as "invoices_query") as "q0" order by "customers___customer_id" asc limit $6',
      );

      // First 5 bindings are for the customerId, last one is for the limit
      assert.deepEqual(query.bindings, [1, 1, 1, 1, 1, 5000]);
    });
  });
});
