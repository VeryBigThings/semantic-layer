import * as assert from "node:assert/strict";
import * as semanticLayer from "../index.js";

import { beforeAll, describe, it } from "vitest";
import { InferSqlQueryResultType, QueryBuilderQuery } from "../index.js";

import fs from "node:fs/promises";
import path from "node:path";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import pg from "pg";
import { generateErrorMessage } from "zod-error";
import { zodToJsonSchema } from "zod-to-json-schema";

// import { format as sqlFormat } from "sql-formatter";

const __dirname = path.dirname(new URL(import.meta.url).pathname);

describe("semantic layer", async () => {
  let client: pg.Client;

  beforeAll(async () => {
    const bootstrapSql = await fs.readFile(
      path.join(__dirname, "sqls/Chinook_PostgreSql.sql"),
      "utf-8",
    );

    const container = await new PostgreSqlContainer().start();

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

    return async () => {
      await client.end();
      await container.stop();
    };
  }, 60000);

  describe("models from tables", async () => {
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
        sql: ({ model, sql }) =>
          sql`COUNT(DISTINCT ${model.column("CustomerId")})`,
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
        sql: ({ model, sql }) =>
          sql`SUM(COALESCE(${model.column("Total")}, 0))`,
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
        sql: ({ model, sql }) =>
          sql`SUM(COALESCE(${model.column("Quantity")}, 0))`,
      })
      .withMetric("unit_price", {
        type: "string",
        sql: ({ model, sql }) =>
          sql`SUM(COALESCE(${model.column("UnitPrice")}, 0))`,
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

    it("can report errors", async () => {
      const result = queryBuilder.querySchema.safeParse({
        members: ["customers.customer_id", "invoices.total"],
        order: [{ member: "customers.customer_id", direction: "asc" }],
        filters: [
          { operator: "equals", member: "customers.customer_id1", value: 1 },
          { operator: "gte", member: "customers.customer_id", value: ["a"] },
          {
            operator: "nonExistingOperator",
            member: "customers.customer_id2",
            value: 1,
          },
          {
            operator: "inQuery",
            member: "customers.customer_id",
            value: {
              members: ["customers.customer_id"],
              filters: [
                {
                  operator: "equals",
                  member: "customers.customer_id2",
                  value: [1],
                },
              ],
            },
          },
        ],
        limit: 10,
      });

      if (result.success) {
        throw new Error("Expected error");
      }

      const formattedErrors = generateErrorMessage(result.error.issues, {
        delimiter: { error: "\n" },
        code: { enabled: false },
        path: { label: "Error at ", enabled: true, type: "objectNotation" },
        message: { label: "", enabled: true },
        transform: ({ messageComponent, pathComponent }) => {
          return `${pathComponent}: ${messageComponent}`;
        },
      });

      const expectedFormattedErrors = [
        "Error at filters[0].member: Member not found",
        "Error at filters[0].value: Expected array, received number",
        "Error at filters[1].value[0]: Expected number, received nan",
        "Error at filters[2].operator: Invalid discriminator value. Expected 'and' | 'or' | 'equals' | 'in' | 'notEquals' | 'notIn' | 'notSet' | 'set' | 'contains' | 'notContains' | 'startsWith' | 'notStartsWith' | 'endsWith' | 'notEndsWith' | 'gt' | 'gte' | 'lt' | 'lte' | 'inDateRange' | 'notInDateRange' | 'beforeDate' | 'afterDate' | 'inQuery' | 'notInQuery'",
        "Error at filters[3].value.filters[0].member: Member not found",
      ].join("\n");

      assert.deepEqual(formattedErrors, expectedFormattedErrors);
    });

    it("can query one dimension and one metric", async () => {
      const query = queryBuilder.buildQuery({
        members: ["customers.customer_id", "invoices.total"],
        order: [{ member: "customers.customer_id", direction: "asc" }],
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

    it("can query one dimension and multiple metrics", async () => {
      const query = queryBuilder.buildQuery({
        members: [
          "customers.customer_id",
          "invoices.total",
          "invoice_lines.unit_price",
        ],
        order: [{ member: "customers.customer_id", direction: "asc" }],
        limit: 10,
      });

      const result = await client.query<
        InferSqlQueryResultType<typeof query, { "invoices.total": number }>
      >(query.sql, query.bindings);

      assert.deepEqual(result.rows, [
        {
          customers___customer_id: 1,
          invoices___total: "39.62",
          invoice_lines___unit_price: "39.62",
        },
        {
          customers___customer_id: 2,
          invoices___total: "37.62",
          invoice_lines___unit_price: "37.62",
        },
        {
          customers___customer_id: 3,
          invoices___total: "39.62",
          invoice_lines___unit_price: "39.62",
        },
        {
          customers___customer_id: 4,
          invoices___total: "39.62",
          invoice_lines___unit_price: "39.62",
        },
        {
          customers___customer_id: 5,
          invoices___total: "40.62",
          invoice_lines___unit_price: "40.62",
        },
        {
          customers___customer_id: 6,
          invoices___total: "49.62",
          invoice_lines___unit_price: "49.62",
        },
        {
          customers___customer_id: 7,
          invoices___total: "42.62",
          invoice_lines___unit_price: "42.62",
        },
        {
          customers___customer_id: 8,
          invoices___total: "37.62",
          invoice_lines___unit_price: "37.62",
        },
        {
          customers___customer_id: 9,
          invoices___total: "37.62",
          invoice_lines___unit_price: "37.62",
        },
        {
          customers___customer_id: 10,
          invoices___total: "37.62",
          invoice_lines___unit_price: "37.62",
        },
      ]);
    });

    it("can query a metric and slice it correctly by a non primary key dimension", async () => {
      const query = queryBuilder.buildQuery({
        members: ["customers.country", "customers.count"],
        order: [{ member: "customers.country", direction: "asc" }],
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

    it("will correctly load distinct dimensions when no metrics are loaded", async () => {
      const query = queryBuilder.buildQuery({
        members: ["customers.country"],
        order: [{ member: "customers.country", direction: "asc" }],
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

    it("will remove non projected members from order clause", async () => {
      const query = queryBuilder.buildQuery({
        members: [
          "customers.customer_id",
          "customers.full_name",
          "invoice_lines.invoice_id",
        ],
        limit: 10,
        order: [{ member: "invoices.invoice_date", direction: "asc" }],
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

    it("can query one dimension and metric and filter by a different metric", async () => {
      const query = queryBuilder.buildQuery({
        members: ["customers.customer_id", "invoices.total"],
        order: [{ member: "customers.customer_id", direction: "asc" }],
        limit: 10,
        filters: [
          {
            operator: "lt",
            member: "invoice_lines.unit_price",
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

    it("can query a metric and filter by a dimension", async () => {
      const query = queryBuilder.buildQuery({
        members: ["invoices.total"],
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

    it("can query multiple metrics and filter by a dimension", async () => {
      const query = queryBuilder.buildQuery({
        members: ["invoices.total", "invoice_lines.quantity"],
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

    it("can query dimensions only", async () => {
      const query = queryBuilder.buildQuery({
        members: ["customers.customer_id", "albums.title"],
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

    it("can correctly query datetime granularities", async () => {
      const query = queryBuilder.buildQuery({
        members: [
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

    it("can introspect if dimension is a primary key", () => {
      assert.ok(
        repository.getDimension("customers.customer_id").isPrimaryKey(),
      );
    });

    it("can introspect if dimension is a granularity", () => {
      assert.ok(
        repository
          .getDimension("invoices.invoice_date.day_of_month")
          .isGranularity(),
      );
    });

    it("can filter by results of another query", async () => {
      const query = queryBuilder.buildQuery({
        members: ["customers.country"],
        order: [{ member: "customers.country", direction: "asc" }],
        filters: [
          {
            operator: "inQuery",
            member: "customers.country",
            value: {
              members: ["customers.country"],
              filters: [
                {
                  operator: "equals",
                  member: "customers.country",
                  value: ["Argentina"],
                },
              ],
            },
          },
        ],
        limit: 10,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [{ customers___country: "Argentina" }]);
    });
  });

  describe("models from sql queries", async () => {
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
        sql: ({ model, sql }) =>
          sql`SUM(COALESCE(${model.column("Total")}, 0))`,
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

    it("can query one dimension and multiple metrics", async () => {
      const query = queryBuilder.buildQuery({
        members: ["customers.customer_id", "invoices.total"],
        order: [{ member: "customers.customer_id", direction: "asc" }],
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

  describe("query schema", async () => {
    it("can parse a valid query", () => {
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
          sql: ({ model, sql }) =>
            sql`SUM(COALESCE(${model.column("Total")}, 0))`,
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
        members: ["customers.customer_id", "invoices.total"],
        order: [{ member: "customers.customer_id", direction: "asc" }],
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
          members: {
            type: "array",
            items: {
              type: "string",
              description: "Dimension or metric name",
            },
            minItems: 1,
          },
          limit: {
            type: "number",
          },
          offset: {
            type: "number",
          },
          order: {
            type: "array",
            items: {
              type: "object",
              properties: {
                member: {
                  type: "string",
                },
                direction: {
                  type: "string",
                  enum: ["asc", "desc"],
                },
              },
              required: ["member", "direction"],
              additionalProperties: false,
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
                  description: "AND connective for filters",
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
                  description: "OR connective for filters",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that are equal to the given value. Accepts an array of values. If the array contains more than one value, the filter will return rows where the member is equal to any of the values.",
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "in",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that are equal to the given value. Accepts an array of values. If the array contains more than one value, the filter will return rows where the member is equal to any of the values.",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that are not equal to the given value. Accepts an array of values. If the array contains more than one value, the filter will return rows where the member is not equal to any of the values.",
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "notIn",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that are not equal to the given value. Accepts an array of values. If the array contains more than one value, the filter will return rows where the member is not equal to any of the values.",
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
                  description: "Filter for values that are not set.",
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
                  description: "Filter for values that are set.",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that contain the given string. Accepts an array of strings.",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that do not contain the given string. Accepts an array of strings.",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that start with the given string. Accepts an array of strings.",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that do not start with the given string. Accepts an array of strings.",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that end with the given string. Accepts an array of strings.",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that do not end with the given string. Accepts an array of strings.",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that are greater than the given value.",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that are greater than or equal to the given value.",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that are less than the given value.",
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
                      minItems: 1,
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that are less than or equal to the given value.",
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
                  description:
                    "Filter for dates in the given range. Accepts a value as date range, date range formatted as a string or an object with startDate and endDate properties.",
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
                      $ref: "#/properties/filters/items/anyOf/18/properties/value",
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for dates not in the given range. Accepts a value as date range, date range formatted as a string or an object with startDate and endDate properties.",
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
                  description:
                    'Filter for dates before the given date. Accepts a value as date, date formatted as a string or a string with relative time like "start of last year".',
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
                      $ref: "#/properties/filters/items/anyOf/20/properties/value",
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    'Filter for dates after the given date. Accepts a value as date, date formatted as a string or a string with relative time like "start of last year".',
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "inQuery",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      $ref: "#",
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that are in the result of the given query.",
                },
                {
                  type: "object",
                  properties: {
                    operator: {
                      type: "string",
                      const: "notInQuery",
                    },
                    member: {
                      type: "string",
                    },
                    value: {
                      $ref: "#",
                    },
                  },
                  required: ["operator", "member", "value"],
                  additionalProperties: false,
                  description:
                    "Filter for values that are not in the result of the given query.",
                },
              ],
              description:
                "Query filters. Top level filters are connected with AND connective. Filters can be nested with AND and OR connectives.",
            },
          },
        },
        required: ["members"],
        additionalProperties: false,
        description: "Query schema",
        $schema: "http://json-schema.org/draft-07/schema#",
      });
    });
  });

  describe("model descriptions and query introspection", async () => {
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
        format: "percentage",
        sql: ({ model, sql }) =>
          sql`SUM(COALESCE(${model.column("Total")}, 0))`,
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

    it("allows access to the model descriptions", () => {
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

    it("allows introspection of a query", () => {
      const query: QueryBuilderQuery<typeof queryBuilder> = {
        members: [
          "customers.customer_id",
          "invoices.invoice_id",
          "invoices.customer_id",
          "invoices.total",
        ],
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

  describe("full repository", async () => {
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
      .withMetric("total", {
        type: "number",
        description: "Invoice total.",
        sql: ({ model, sql }) =>
          sql`SUM(COALESCE, ${model.column("Total")}, 0))`,
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
        type: "number",
        description: "Sum of the track quantities across models.",
        sql: ({ model, sql }) =>
          sql`SUM(COALESCE(${model.column("Quantity")}, 0))`,
      })
      .withMetric("unit_price", {
        type: "number",
        description: "Sum of the track unit prices across models.",
        sql: ({ model, sql }) =>
          sql`SUM(COALESCE(${model.column("UnitPrice")}, 0))`,
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
      .withMetric("unit_price", {
        type: "number",
        description: "Sum of the track unit prices across models.",
        sql: ({ model, sql }) =>
          sql`SUM(COALESCE(${model.column("UnitPrice")}, 0))`,
        format: (value) => `Price: $${value}`,
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
        format: (value) => `Artist: ${value}`,
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

    it("should return distinct results for dimension only query", async () => {
      const query = queryBuilder.buildQuery({
        members: ["artists.name"],
        filters: [
          {
            operator: "equals",
            member: "genres.name",
            value: ["Rock"],
          },
        ],
        order: [{ member: "artists.name", direction: "asc" }],
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

    it("should return order results by default", async () => {
      const query = queryBuilder.buildQuery({
        members: ["artists.name"],
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

    it("parsed query should equal original query", async () => {
      const query = {
        members: ["artists.name", "tracks.unit_price"],
        filters: [
          {
            operator: "equals",
            member: "genres.name",
            value: ["Rock"],
          },
        ],
        order: [{ member: "artists.name", direction: "asc" }],
        limit: 10,
      };

      const parsedQuery = queryBuilder.querySchema.parse(query);

      assert.deepEqual(parsedQuery, query);
    });

    it("can filter by contains", async () => {
      const query = queryBuilder.buildQuery({
        members: ["artists.name"],
        filters: [
          {
            operator: "contains",
            member: "artists.name",
            value: ["ac"],
          },
        ],
        order: [{ member: "artists.name", direction: "asc" }],
        limit: 1,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          artists___name: "AC/DC",
        },
      ]);
    });

    it("can filter by notContains", async () => {
      const query = queryBuilder.buildQuery({
        members: ["artists.name"],
        filters: [
          {
            operator: "notContains",
            member: "artists.name",
            value: ["cor"],
          },
        ],
        order: [{ member: "artists.name", direction: "asc" }],
        limit: 1,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          artists___name: "AC/DC",
        },
      ]);
    });

    it("can filter by startsWith", async () => {
      const query = queryBuilder.buildQuery({
        members: ["artists.name"],
        filters: [
          {
            operator: "startsWith",
            member: "artists.name",
            value: ["ac"],
          },
        ],
        order: [{ member: "artists.name", direction: "asc" }],
        limit: 1,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          artists___name: "AC/DC",
        },
      ]);
    });

    it("can filter by notStartsWith", async () => {
      const query = queryBuilder.buildQuery({
        members: ["artists.name"],
        filters: [
          {
            operator: "notStartsWith",
            member: "artists.name",
            value: ["a cor"],
          },
        ],
        order: [{ member: "artists.name", direction: "asc" }],
        limit: 1,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          artists___name: "AC/DC",
        },
      ]);
    });

    it("can filter by endsWith", async () => {
      const query = queryBuilder.buildQuery({
        members: ["artists.name"],
        filters: [
          {
            operator: "endsWith",
            member: "artists.name",
            value: ["dc"],
          },
        ],
        order: [{ member: "artists.name", direction: "asc" }],
        limit: 1,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          artists___name: "AC/DC",
        },
      ]);
    });

    it("can filter by notEndsWith", async () => {
      const query = queryBuilder.buildQuery({
        members: ["artists.name"],
        filters: [
          {
            operator: "notEndsWith",
            member: "artists.name",
            value: ["som"],
          },
        ],
        order: [{ member: "artists.name", direction: "asc" }],
        limit: 1,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          artists___name: "AC/DC",
        },
      ]);
    });

    it("can return formatting function from introspection", async () => {
      const queryInput: QueryBuilderQuery<typeof queryBuilder> = {
        members: ["artists.name", "tracks.unit_price"],
        filters: [
          {
            operator: "equals",
            member: "genres.name",
            value: ["Rock"],
          },
        ],
        order: [{ member: "artists.name", direction: "asc" }],
        limit: 1,
      };

      const query = queryBuilder.buildQuery(queryInput);

      const result = await client.query(query.sql, query.bindings);

      const introspection = queryBuilder.introspect(queryInput);

      const formattedResult = result.rows.map((row) =>
        Object.fromEntries(
          Object.entries(introspection).map(([column, columnIntrospection]) => {
            const { format } = columnIntrospection;
            if (format && format instanceof Function) {
              return [column, format(row[column])];
            }
            return [column, row[column]];
          }),
        ),
      );

      assert.deepEqual(formattedResult, [
        {
          artists___name: "Artist: AC/DC",
          tracks___unit_price: "Price: $17.82",
        },
      ]);
    });
  });

  describe("repository with context", async () => {
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

    it("propagates context to all sql functions", async () => {
      const queryBuilder = repository.build("postgresql");
      const query = queryBuilder.buildQuery(
        {
          members: ["customers.customer_id", "invoices.invoice_id"],
        },
        { customerId: 1 },
      );

      assert.equal(
        query.sql,
        'select "q0"."customers___customer_id" as "customers___customer_id", "q0"."invoices___invoice_id" as "invoices___invoice_id" from (select "invoices_query"."customers___customer_id" as "customers___customer_id", "invoices_query"."invoices___invoice_id" as "invoices___invoice_id" from (select distinct "Invoice"."InvoiceId" as "invoices___invoice_id", "customers"."CustomerId" || cast($1 as text) as "customers___customer_id" from "Invoice" right join (select * from "Customer" where "CustomerId" = $2) as "customers" on "customers"."CustomerId" || cast($3 as text) = "Invoice"."CustomerId" and $4 = $5) as "invoices_query") as "q0" order by "customers___customer_id" asc limit $6 offset $7',
      );

      // First 5 bindings are for the customerId, last one is for the limit
      assert.deepEqual(query.bindings, [1, 1, 1, 1, 1, 5000, 0]);
    });

    it("propagates context to query filters", async () => {
      const queryBuilder = repository.build("postgresql");
      const query = queryBuilder.buildQuery(
        {
          members: ["customers.customer_id", "invoices.invoice_id"],
          filters: [
            {
              operator: "inQuery",
              member: "customers.customer_id",
              value: {
                members: ["customers.customer_id"],
                filters: [
                  {
                    operator: "equals",
                    member: "customers.customer_id",
                    value: [1],
                  },
                ],
              },
            },
          ],
        },
        { customerId: 1 },
      );

      assert.equal(
        query.sql,
        'select "q0"."customers___customer_id" as "customers___customer_id", "q0"."invoices___invoice_id" as "invoices___invoice_id" from (select "invoices_query"."customers___customer_id" as "customers___customer_id", "invoices_query"."invoices___invoice_id" as "invoices___invoice_id" from (select distinct "Invoice"."InvoiceId" as "invoices___invoice_id", "customers"."CustomerId" || cast($1 as text) as "customers___customer_id" from "Invoice" right join (select * from "Customer" where "CustomerId" = $2) as "customers" on "customers"."CustomerId" || cast($3 as text) = "Invoice"."CustomerId" and $4 = $5 where "customers"."CustomerId" || cast($6 as text) in (select "q0"."customers___customer_id" as "customers___customer_id" from (select "customers_query"."customers___customer_id" as "customers___customer_id" from (select distinct "customers"."CustomerId" || cast($7 as text) as "customers___customer_id" from (select * from "Customer" where "CustomerId" = $8) as "customers" where "customers"."CustomerId" || cast($9 as text) = $10) as "customers_query") as "q0" order by "customers___customer_id" asc limit $11 offset $12)) as "invoices_query") as "q0" order by "customers___customer_id" asc limit $13 offset $14',
      );

      assert.deepEqual(
        query.bindings,
        [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5000, 0, 5000, 0],
      );
    });
  });

  describe("tables with schemas", async () => {
    type QueryContext = {
      schema: string;
    };

    const customersModel = semanticLayer
      .model<QueryContext>()
      .withName("customers")
      .fromTable("public.Customer")
      .withDimension("customer_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model }) => model.column("CustomerId"),
      });

    const invoicesModel = semanticLayer
      .model<QueryContext>()
      .withName("invoices")
      .fromSqlQuery(
        ({ sql, identifier, getContext }) =>
          sql`select * from ${identifier(getContext().schema)}.${identifier(
            "Invoice",
          )}`,
      )
      .withDimension("invoice_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model }) => model.column("InvoiceId"),
      })
      .withDimension("customer_id", {
        type: "number",
        sql: ({ model }) => model.column("CustomerId"),
      });

    const invoiceLinesModel = semanticLayer
      .model<QueryContext>()
      .withName("invoice_lines")
      .fromTable(
        ({ sql, identifier, getContext }) =>
          sql`${identifier(getContext().schema)}.${identifier("InvoiceLine")}`,
      )
      .withDimension("invoice_line_id", {
        type: "number",
        primaryKey: true,
        sql: ({ model }) => model.column("InvoiceLineId"),
      })
      .withDimension("invoice_id", {
        type: "number",
        sql: ({ model }) => model.column("InvoiceId"),
      });

    const repository = semanticLayer
      .repository<QueryContext>()
      .withModel(customersModel)
      .withModel(invoicesModel)
      .withModel(invoiceLinesModel)
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
      );

    it("can build SQL with namespaced tables (1)", async () => {
      const queryBuilder = repository.build("postgresql");
      const query = queryBuilder.buildQuery(
        {
          members: [
            "customers.customer_id",
            "invoices.invoice_id",
            "invoice_lines.invoice_line_id",
          ],
        },
        { schema: "public" },
      );

      assert.equal(
        query.sql,
        'select "q0"."customers___customer_id" as "customers___customer_id", "q0"."invoices___invoice_id" as "invoices___invoice_id", "q0"."invoice_lines___invoice_line_id" as "invoice_lines___invoice_line_id" from (select "invoice_lines_query"."customers___customer_id" as "customers___customer_id", "invoice_lines_query"."invoices___invoice_id" as "invoices___invoice_id", "invoice_lines_query"."invoice_lines___invoice_line_id" as "invoice_lines___invoice_line_id" from (select distinct "public"."InvoiceLine"."InvoiceLineId" as "invoice_lines___invoice_line_id", "invoices"."InvoiceId" as "invoices___invoice_id", "public"."Customer"."CustomerId" as "customers___customer_id" from "public"."InvoiceLine" right join (select * from "public"."Invoice") as "invoices" on "invoices"."InvoiceId" = "public"."InvoiceLine"."InvoiceId" right join "public"."Customer" on "public"."Customer"."CustomerId" = "invoices"."CustomerId") as "invoice_lines_query") as "q0" order by "customers___customer_id" asc limit $1 offset $2',
      );

      assert.deepEqual(query.bindings, [5000, 0]);
    });

    it("can build SQL with namespaced tables (2)", async () => {
      const queryBuilder = repository.build("postgresql");
      const query = queryBuilder.buildQuery(
        {
          members: ["invoices.invoice_id"],
        },
        { schema: "public" },
      );

      assert.equal(
        query.sql,
        'select "q0"."invoices___invoice_id" as "invoices___invoice_id" from (select "invoices_query"."invoices___invoice_id" as "invoices___invoice_id" from (select distinct "invoices"."InvoiceId" as "invoices___invoice_id" from (select * from "public"."Invoice") as "invoices") as "invoices_query") as "q0" order by "invoices___invoice_id" asc limit $1 offset $2',
      );

      assert.deepEqual(query.bindings, [5000, 0]);
    });

    it("can build SQL for ANSI", () => {
      const ansiQueryBuilder = repository.build("ansi");
      const query = ansiQueryBuilder.buildQuery(
        {
          members: ["invoices.invoice_id"],
        },
        { schema: "public" },
      );

      assert.equal(
        query.sql,
        'select "q0"."invoices___invoice_id" as "invoices___invoice_id" from (select "invoices_query"."invoices___invoice_id" as "invoices___invoice_id" from (select distinct "invoices"."InvoiceId" as "invoices___invoice_id" from (select * from "public"."Invoice") as "invoices") as "invoices_query") as "q0" order by "invoices___invoice_id" asc limit ? offset ?',
      );

      assert.deepEqual(query.bindings, [5000, 0]);
    });

    it("can build SQL for Databricks", () => {
      const ansiQueryBuilder = repository.build("databricks");
      const query = ansiQueryBuilder.buildQuery(
        {
          members: ["invoices.invoice_id"],
        },
        { schema: "public" },
      );

      assert.equal(
        query.sql,
        "select `q0`.`invoices___invoice_id` as `invoices___invoice_id` from (select `invoices_query`.`invoices___invoice_id` as `invoices___invoice_id` from (select distinct `invoices`.`InvoiceId` as `invoices___invoice_id` from (select * from `public`.`Invoice`) as `invoices`) as `invoices_query`) as `q0` order by `invoices___invoice_id` asc limit ? offset ?",
      );

      assert.deepEqual(query.bindings, [5000, 0]);
    });
  });

  describe("repository without custom SQL", async () => {
    const customersModel = semanticLayer
      .model()
      .withName("Customer")
      .fromTable("Customer")
      .withDimension("CustomerId", {
        type: "number",
        primaryKey: true,
      })
      .withDimension("FirstName", {
        type: "string",
      })
      .withDimension("LastName", {
        type: "string",
      })

      .withMetric("Count", {
        type: "string",
        sql: ({ model, sql }) =>
          sql`COUNT(DISTINCT ${model.column("CustomerId")})`,
      });

    const invoicesModel = semanticLayer
      .model()
      .withName("Invoice")
      .fromTable("Invoice")
      .withDimension("InvoiceId", {
        type: "number",
        primaryKey: true,
      })
      .withDimension("CustomerId", {
        type: "number",
      })
      .withDimension("InvoiceDate", {
        type: "datetime",
      })
      .withMetric("Total", {
        type: "string",
        sql: ({ model, sql }) =>
          sql`SUM(COALESCE(${model.column("Total")}, 0))`,
      });

    const repository = semanticLayer
      .repository()
      .withModel(customersModel)
      .withModel(invoicesModel)
      .joinOneToMany(
        "Customer",
        "Invoice",
        ({ sql, models }) =>
          sql`${models.Customer.dimension(
            "CustomerId",
          )} = ${models.Invoice.dimension("CustomerId")}`,
      );

    const queryBuilder = repository.build("postgresql");

    it("generates correct SQL", async () => {
      const query = queryBuilder.buildQuery({
        members: ["Customer.CustomerId", "Invoice.InvoiceId", "Invoice.Total"],
        order: [{ member: "Customer.CustomerId", direction: "asc" }],
        filters: [
          { operator: "equals", member: "Customer.CustomerId", value: [1] },
          { operator: "equals", member: "Invoice.InvoiceId", value: [98] },
        ],
        limit: 10,
      });

      const result = await client.query<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result.rows, [
        {
          Customer___CustomerId: 1,
          Invoice___Total: "3.98",
          Invoice___InvoiceId: 98,
        },
      ]);
    });
  });
});
