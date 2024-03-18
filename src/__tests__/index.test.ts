import * as assert from "node:assert/strict";
import * as C from "../index.js";

import { describe, it } from "node:test";

import { format as sqlFormat } from "sql-formatter";

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
  .withDimension("rn", {
    type: "number",
    sql: ({ table, sql }) =>
      sql`dense_rank() over (partition by ${table.dimension(
        "invoice_date.year",
      )} order by ${table.dimension("invoice_date")})`,
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
  .withMetric("unit_price", {
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

const query = db.query({
  dimensions: [
    "Customer.customer_id",
    "Customer.full_name",
    "Invoice.invoice_date.year",
    "Invoice.rn",
    /*'InvoiceLine.invoice_line_id',
    'Invoice.invoice_id',
    'Track.track_id,'*/
    //'Album.title',
  ],
  metrics: ["InvoiceLine.unit_price", "Invoice.total"],
  filters: [
    {
      operator: "inDateRange",
      member: "Invoice.invoice_date",
      value: "from Jan 1st 2011 at 00:00 to Dec 31th 2012 23:00",
    },
    /*{ operator: 'set', member: 'Customer.customer_id' },
    {
      operator: 'notContains',
      member: 'InvoiceLine.unit_price',
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
    },*/
  ],
  order: {
    /* 'InvoiceLine.unit_price': 'asc',  'Customer.customer_id': 'asc',*/
    "Invoice.invoice_date.year": "desc",
    "Invoice.rn": "asc",
  },
});

await describe("foobar()", async () => {
  await describe("given two positive integers", async () => {
    await describe("when called", async () => {
      await it("returns the sum of them multiplied by 3", () => {
        // biome-ignore lint/suspicious/noConsoleLog: <explanation>
        console.log(
          sqlFormat(query.sql, {
            language: "postgresql",
            keywordCase: "upper",
          }),
        );
        assert.equal(1, 1);
      });
    });
  });
});
