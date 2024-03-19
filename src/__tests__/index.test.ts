import * as assert from "node:assert/strict";
import * as C from "../index.js";

import { after, before, describe, it } from "node:test";
import {
  PostgreSqlContainer,
  StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";

import fs from "node:fs/promises";
import path from "node:path";
import pg from "pg";

//import { format as sqlFormat } from "sql-formatter";

const __dirname = path.dirname(new URL(import.meta.url).pathname);

const customersTable = C.table("Customer")
  .withDimension("customer_id", {
    type: "number",
    primaryKey: true,
    sql: ({ table, sql }) => sql`${table.column("CustomerId")}`,
  })
  .withDimension("first_name", {
    type: "string",
    sql: ({ table }) => table.column("FirstName"),
  })
  .withDimension("last_name", {
    type: "string",
    sql: ({ table }) => table.column("LastName"),
  })
  .withDimension("company", {
    type: "string",
    sql: ({ table }) => table.column("Company"),
  })
  .withDimension("full_name", {
    type: "string",
    sql: ({ table, sql }) =>
      sql`${table.dimension("first_name")} || ' ' || ${table.dimension(
        "last_name",
      )}`,
  });

const invoicesTable = C.table("Invoice")
  .withDimension("invoice_id", {
    type: "number",
    primaryKey: true,
    sql: ({ table }) => table.column("InvoiceId"),
  })
  .withDimension("customer_id", {
    type: "number",
    sql: ({ table }) => table.column("CustomerId"),
  })
  .withDimension("invoice_date", {
    type: "date",
    sql: ({ table }) => table.column("InvoiceDate"),
  })
  .withMetric("total", {
    type: "sum",
    sql: ({ table }) => table.column("Total"),
  });

const invoiceLinesTable = C.table("InvoiceLine")
  .withDimension("invoice_line_id", {
    type: "number",
    primaryKey: true,
    sql: ({ table }) => table.column("InvoiceLineId"),
  })
  .withDimension("invoice_id", {
    type: "number",
    sql: ({ table }) => table.column("InvoiceId"),
  })
  .withDimension("track_id", {
    type: "number",
    sql: ({ table }) => table.column("TrackId"),
  })
  .withMetric("quantity", {
    type: "sum",
    sql: ({ table }) => table.column("Quantity"),
  })
  .withMetric("total_unit_price", {
    type: "sum",
    sql: ({ table }) => table.column("UnitPrice"),
  });

const tracksTable = C.table("Track")
  .withDimension("track_id", {
    type: "number",
    primaryKey: true,
    sql: ({ table }) => table.column("TrackId"),
  })
  .withDimension("name", {
    type: "string",
    sql: ({ table }) => table.column("Name"),
  })
  .withDimension("album_id", {
    type: "number",
    sql: ({ table }) => table.column("AlbumId"),
  });

const albumsTable = C.table("Album")
  .withDimension("album_id", {
    type: "number",
    primaryKey: true,
    sql: ({ table }) => table.column("AlbumId"),
  })
  .withDimension("title", {
    type: "string",
    sql: ({ table }) => table.column("Title"),
  });

const db = C.database()
  .withTable(customersTable)
  .withTable(invoicesTable)
  .withTable(invoiceLinesTable)
  .withTable(tracksTable)
  .withTable(albumsTable)
  .joinOneToMany(
    "Customer",
    "Invoice",
    ({ sql, dimensions }) =>
      sql`${dimensions.Customer.customer_id} = ${dimensions.Invoice.customer_id}`,
  )
  .joinOneToMany(
    "Invoice",
    "InvoiceLine",
    ({ sql, dimensions }) =>
      sql`${dimensions.Invoice.invoice_id} = ${dimensions.InvoiceLine.invoice_id}`,
  )
  .joinOneToMany(
    "InvoiceLine",
    "Track",
    ({ sql, dimensions }) =>
      sql`${dimensions.InvoiceLine.track_id} = ${dimensions.Track.track_id}`,
  )
  .joinManyToMany(
    "Track",
    "Album",
    ({ sql, dimensions }) =>
      sql`${dimensions.Track.album_id} = ${dimensions.Album.album_id}`,
  );

/*const query = db.query({
  dimensions: [
    "Customer.customer_id",
    //'InvoiceLine.invoice_line_id',
    //'Invoice.invoice_id',
    //'Track.track_id,',
    //'Album.title',
  ],
  metrics: ["InvoiceLine.total_unit_price", "Invoice.total"],
  filters: [
    {
      operator: "inDateRange",
      member: "Invoice.invoice_date",
      value: "from Jan 1st 2011 at 00:00 to Dec 31th 2012 23:00",
    },
    { operator: 'set', member: 'Customer.customer_id' },
    {
      operator: 'notContains',
      member: 'InvoiceLine.total_unit_price',
      value: ['0.99', '1'],
    },
    { operator: 'notEquals', member: 'Invoice.total', value: ['0.99'] },
    {
      operator: 'or',
      filters: [
        { operator: 'notEquals', member: 'Invoice.invoice_id', value: ['1'] },
        {
          operator: 'notEquals',
          member: 'InvoiceLine.invoice_line_id',
          value: ['3'],
        },
      ],
    },
  ],
  order: {
    // 'InvoiceLine.unit_price': 'asc',  'Customer.customer_id': 'asc',
    "Invoice.invoice_date.year": "desc",
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

  await it("can query one dimension and one metric", async () => {
    const query = db.query({
      dimensions: ["Customer.customer_id"],
      metrics: ["Invoice.total"],
      order: { "Customer.customer_id": "asc" },
      limit: 10,
    });

    const result = await client.query(query.sql, query.bindings);

    assert.deepEqual(result.rows, [
      { Customer___customer_id: 1, Invoice___total: "39.62" },
      { Customer___customer_id: 2, Invoice___total: "37.62" },
      { Customer___customer_id: 3, Invoice___total: "39.62" },
      { Customer___customer_id: 4, Invoice___total: "39.62" },
      { Customer___customer_id: 5, Invoice___total: "40.62" },
      { Customer___customer_id: 6, Invoice___total: "49.62" },
      { Customer___customer_id: 7, Invoice___total: "42.62" },
      { Customer___customer_id: 8, Invoice___total: "37.62" },
      { Customer___customer_id: 9, Invoice___total: "37.62" },
      { Customer___customer_id: 10, Invoice___total: "37.62" },
    ]);
  });

  await it("can query one dimension and multiple metrics", async () => {
    const query = db.query({
      dimensions: ["Customer.customer_id"],
      metrics: ["Invoice.total", "InvoiceLine.total_unit_price"],
      order: { "Customer.customer_id": "asc" },
      limit: 10,
    });

    const result = await client.query(query.sql, query.bindings);

    assert.deepEqual(result.rows, [
      {
        Customer___customer_id: 1,
        Invoice___total: "39.62",
        InvoiceLine___total_unit_price: "39.62",
      },
      {
        Customer___customer_id: 2,
        Invoice___total: "37.62",
        InvoiceLine___total_unit_price: "37.62",
      },
      {
        Customer___customer_id: 3,
        Invoice___total: "39.62",
        InvoiceLine___total_unit_price: "39.62",
      },
      {
        Customer___customer_id: 4,
        Invoice___total: "39.62",
        InvoiceLine___total_unit_price: "39.62",
      },
      {
        Customer___customer_id: 5,
        Invoice___total: "40.62",
        InvoiceLine___total_unit_price: "40.62",
      },
      {
        Customer___customer_id: 6,
        Invoice___total: "49.62",
        InvoiceLine___total_unit_price: "49.62",
      },
      {
        Customer___customer_id: 7,
        Invoice___total: "42.62",
        InvoiceLine___total_unit_price: "42.62",
      },
      {
        Customer___customer_id: 8,
        Invoice___total: "37.62",
        InvoiceLine___total_unit_price: "37.62",
      },
      {
        Customer___customer_id: 9,
        Invoice___total: "37.62",
        InvoiceLine___total_unit_price: "37.62",
      },
      {
        Customer___customer_id: 10,
        Invoice___total: "37.62",
        InvoiceLine___total_unit_price: "37.62",
      },
    ]);
  });

  await it("can query one dimension and metric and filter by a different metric", async () => {
    const query = db.query({
      dimensions: ["Customer.customer_id"],
      metrics: ["Invoice.total"],
      order: { "Customer.customer_id": "asc" },
      limit: 10,
      filters: [
        { operator: "lt", member: "InvoiceLine.total_unit_price", value: [38] },
      ],
    });

    const result = await client.query(query.sql, query.bindings);

    assert.deepEqual(result.rows, [
      { Customer___customer_id: 2, Invoice___total: "37.62" },
      { Customer___customer_id: 8, Invoice___total: "37.62" },
      { Customer___customer_id: 9, Invoice___total: "37.62" },
      { Customer___customer_id: 10, Invoice___total: "37.62" },
      { Customer___customer_id: 11, Invoice___total: "37.62" },
      { Customer___customer_id: 12, Invoice___total: "37.62" },
      { Customer___customer_id: 13, Invoice___total: "37.62" },
      { Customer___customer_id: 14, Invoice___total: "37.62" },
      { Customer___customer_id: 16, Invoice___total: "37.62" },
      { Customer___customer_id: 18, Invoice___total: "37.62" },
    ]);
  });

  await it("can query a metric and filter by a dimension", async () => {
    const query = db.query({
      metrics: ["Invoice.total"],
      filters: [
        { operator: "equals", member: "Customer.customer_id", value: [1] },
      ],
    });

    const result = await client.query(query.sql, query.bindings);

    assert.deepEqual(result.rows, [{ Invoice___total: "39.62" }]);
  });

  await it("can query multiple metrics and filter by a dimension", async () => {
    const query = db.query({
      metrics: ["Invoice.total", "InvoiceLine.quantity"],
      filters: [
        { operator: "equals", member: "Customer.customer_id", value: [1] },
      ],
    });

    const result = await client.query(query.sql, query.bindings);

    assert.deepEqual(result.rows, [
      { Invoice___total: "39.62", InvoiceLine___quantity: "38" },
    ]);
  });

  await it("can query dimensions only", async () => {
    const query = db.query({
      dimensions: ["Customer.customer_id", "Album.title"],
      filters: [
        { operator: "equals", member: "Customer.customer_id", value: [1] },
      ],
    });

    const result = await client.query(query.sql, query.bindings);

    assert.deepEqual(result.rows, [
      { Customer___customer_id: 1, Album___title: "Ac�stico MTV" },
      { Customer___customer_id: 1, Album___title: "Ac�stico MTV [Live]" },
      { Customer___customer_id: 1, Album___title: "Afrociberdelia" },
      { Customer___customer_id: 1, Album___title: "Appetite for Destruction" },
      { Customer___customer_id: 1, Album___title: "Arquivo II" },
      {
        Customer___customer_id: 1,
        Album___title: "Arquivo Os Paralamas Do Sucesso",
      },
      { Customer___customer_id: 1, Album___title: "Ax� Bahia 2001" },
      {
        Customer___customer_id: 1,
        Album___title: "BBC Sessions [Disc 1] [Live]",
      },
      {
        Customer___customer_id: 1,
        Album___title: "Battlestar Galactica (Classic), Season 1",
      },
      { Customer___customer_id: 1, Album___title: "Bongo Fury" },
      { Customer___customer_id: 1, Album___title: "Carnaval 2001" },
      { Customer___customer_id: 1, Album___title: "Chill: Brazil (Disc 1)" },
      { Customer___customer_id: 1, Album___title: "Cidade Negra - Hits" },
      { Customer___customer_id: 1, Album___title: "Da Lama Ao Caos" },
      { Customer___customer_id: 1, Album___title: "Greatest Kiss" },
      { Customer___customer_id: 1, Album___title: "Na Pista" },
      {
        Customer___customer_id: 1,
        Album___title: "No More Tears (Remastered)",
      },
      { Customer___customer_id: 1, Album___title: "Rattle And Hum" },
      { Customer___customer_id: 1, Album___title: "Sibelius: Finlandia" },
      {
        Customer___customer_id: 1,
        Album___title: "The World of Classical Favourites",
      },
      { Customer___customer_id: 1, Album___title: "Tribute" },
      { Customer___customer_id: 1, Album___title: "Use Your Illusion I" },
    ]);
  });
});
