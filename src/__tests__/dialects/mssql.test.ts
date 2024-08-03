import * as semanticLayer from "../../index.js";

import { assert, beforeAll, describe, it } from "vitest";

import fs from "node:fs/promises";
import path from "node:path";
import { MSSQLServerContainer } from "@testcontainers/mssqlserver";
import mssql from "mssql";
import { InferSqlQueryResultType } from "../../index.js";

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
    type: "datetime",
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
    sql: ({ model, sql }) => sql`SUM(COALESCE, ${model.column("Total")}, 0))`,
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
    sql: ({ model, sql }) => sql`SUM(COALESCE(${model.column("Quantity")}, 0))`,
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
      sql`${models.tracks.dimension("album_id")} = ${models.albums.dimension(
        "album_id",
      )}`,
  )
  .joinManyToOne(
    "albums",
    "artists",
    ({ sql, models }) =>
      sql`${models.albums.dimension("artist_id")} = ${models.artists.dimension(
        "artist_id",
      )}`,
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
      sql`${models.tracks.dimension("genre_id")} = ${models.genres.dimension(
        "genre_id",
      )}`,
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

const queryBuilder = repository.build("mssql");

async function runQuery<T>(sql: string, bindings: Record<string, unknown>) {
  const request = new mssql.Request();
  for (const [key, value] of Object.entries(bindings)) {
    request.input(key, value);
  }

  const result = await request.query(sql);
  return [...result.recordset] as T[];
}

describe("mssql dialect", () => {
  beforeAll(async () => {
    const bootstrapSql = await fs.readFile(
      path.join(__dirname, "../sqls/Chinook_SqlServer.sql"),
      "utf-8",
    );

    const container = await new MSSQLServerContainer().acceptLicense().start();
    const connectionString = container.getConnectionUri();

    await mssql.connect(connectionString);
    const request = new mssql.Request();

    const blocks = bootstrapSql.split("GO");

    for (const block of blocks) {
      await request.batch(block);
    }

    return async () => {
      await container.stop();
    };
  });

  describe("test", () => {
    it("can retrieve data", async () => {
      const query = queryBuilder.buildQuery({
        members: ["artists.name"],
        order: [{ member: "artists.name", direction: "asc" }],
        limit: 1,
      });
      const { sql, bindings } = query;
      const result = await runQuery<InferSqlQueryResultType<typeof query>>(
        sql,
        bindings,
      );
      assert.deepEqual(result, [{ artists___name: "A Cor Do Som" }]);
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

      const result = await runQuery<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result, [
        {
          invoices___invoice_id: 6,
          invoices___invoice_date: new Date("2021-01-19T00:00:00.000+00:00"),
          invoices___invoice_date___date: new Date(
            "2021-01-19T00:00:00.000+00:00",
          ),
          invoices___invoice_date___time: "00:00:00",
          invoices___invoice_date___year: 2021,
          invoices___invoice_date___quarter: "2021-Q1",
          invoices___invoice_date___quarter_of_year: 1,
          invoices___invoice_date___month: "2021-01",
          invoices___invoice_date___month_num: 1,
          invoices___invoice_date___week: "2021-W04",
          invoices___invoice_date___week_num: 4,
          invoices___invoice_date___day_of_month: 19,
          invoices___invoice_date___hour: "2021-01-19 00",
          invoices___invoice_date___hour_of_day: 0,
          invoices___invoice_date___minute: "2021-01-19 00:00",
        },
      ]);
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

      console.log(query.sql);
      console.log(query.bindings);

      const result = await runQuery<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result, [
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

      const result = await runQuery<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result, [
        {
          artists___name: "Aaron Copland & London Symphony Orchestra",
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

      const result = await runQuery<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result, [
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

      const result = await runQuery<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result, [
        {
          artists___name: "Aaron Copland & London Symphony Orchestra",
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

      const result = await runQuery<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result, [
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

      const result = await runQuery<InferSqlQueryResultType<typeof query>>(
        query.sql,
        query.bindings,
      );

      assert.deepEqual(result, [
        {
          artists___name: "Aaron Copland & London Symphony Orchestra",
        },
      ]);
    });
  });
});
