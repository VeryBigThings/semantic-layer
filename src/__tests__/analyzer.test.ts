import * as semanticLayer from "../index.js";

import {
  analyzeQuery,
  getQueriesForHierarchy,
} from "../lib/query-builder/analyzer.js";
import { assert, expect, it } from "vitest";

import exp from "constants";

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
  })
  .withMetric("count", {
    type: "number",
    sql: ({ model, sql }) => sql`COUNT(DISTINCT ${model.column("CustomerId")})`,
  })
  .withCategoricalHierarchy("address", ({ element }) => [
    element.fromDimension("country"),
    element.fromDimension("state"),
    element.fromDimension("city"),
    element.fromDimension("postal_code"),
  ])
  .withCategoricalHierarchy("personal_information", ({ element }) => [
    element("personal_information")
      .withDimensions([
        "customer_id",
        "first_name",
        "last_name",
        "full_name",
        "email",
        "fax",
        "phone",
      ])
      .withKey(["customer_id"])
      .withFormat(["full_name"]),
  ])
  .withCategoricalHierarchy("company", ({ element }) => [
    element.fromDimension("company"),
  ]);

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
    sql: ({ model, sql }) => sql`SUM(COALESCE(${model.column("Total")}, 0))`,
    format: "currency",
  })
  .withCategoricalHierarchy("billing_address", ({ element }) => [
    element.fromDimension("billing_country"),
    element.fromDimension("billing_state"),
    element.fromDimension("billing_city"),
    element.fromDimension("billing_postal_code"),
  ]);

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
    format: "currency",
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
  .withMetric("length", {
    type: "number",
    sql: ({ model, sql }) =>
      sql`SUM(COALESCE(${model.column("Milliseconds")}, 0))`,
    format: (value) => `${value} milliseconds`,
  })
  .withMetric("bytes", {
    type: "number",
    sql: ({ model, sql }) => sql`SUM(COALESCE(${model.column("Bytes")}, 0))`,
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
  })
  .withCategoricalHierarchy("artist", ({ element }) => [
    element("artist")
      .withDimensions(["artist_id", "name"])
      .withKey(["artist_id"])
      .withFormat(["name"]),
  ]);

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
  })
  .withCategoricalHierarchy("media_type", ({ element }) => [
    element("media_type")
      .withDimensions(["media_type_id", "name"])
      .withKey(["media_type_id"])
      .withFormat(["name"]),
  ]);

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
  })
  .withCategoricalHierarchy("genre", ({ element }) => [
    element("genre")
      .withDimensions(["genre_id", "name"])
      .withKey(["genre_id"])
      .withFormat(["name"]),
  ]);

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
  })
  .withCategoricalHierarchy("playlist", ({ element }) => [
    element("playlist")
      .withDimensions(["playlist_id", "name"])
      .withKey(["playlist_id"])
      .withFormat(["name"]),
  ]);

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

export const repository = semanticLayer
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
  .withCategoricalHierarchy("artist", ({ element }) => [
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
  ])
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

export const queryBuilder = repository.build("postgresql");

it("can analyze a query", () => {
  const query: semanticLayer.QueryBuilderQuery<typeof queryBuilder> = {
    members: ["artists.name", "invoices.invoice_date", "invoices.total"],
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

  const queryAnalysis = analyzeQuery(queryBuilder, query);

  expect(queryAnalysis).toMatchObject({
    query,
    dimensions: ["artists.name", "invoices.invoice_date"],
    metrics: ["invoices.total"],
    hierarchies: {
      categorical: [{ name: "artist" }, { name: "artists.artist" }],
      temporal: [{ name: "invoices.invoice_date" }],
      all: [
        { name: "artist" },
        { name: "artists.artist" },
        { name: "invoices.invoice_date" },
      ],
    },
  });
});

it("can generate queries for a hierarchy", () => {
  const query: semanticLayer.QueryBuilderQuery<typeof queryBuilder> = {
    members: [
      "artists.name",
      "albums.title",
      "tracks.track_id",
      "customers.full_name",
      "customers.email",
      "customers.phone",
      "invoices.invoice_date",
      "invoices.total",
    ],
    filters: [
      {
        operator: "equals",
        member: "genres.name",
        value: ["Rock"],
      },
    ],
    order: [{ member: "artists.name", direction: "asc" }],
  };

  const queryAnalysis = analyzeQuery(queryBuilder, query);

  const queriesForHierarchyArtist = getQueriesForHierarchy(
    queryAnalysis,
    "artist",
  );

  expect(queriesForHierarchyArtist).toMatchObject({
    restMembers: [
      "customers.full_name",
      "customers.email",
      "customers.phone",
      "invoices.invoice_date",
      "tracks.track_id",
      "invoices.total",
    ],
    queriesInfo: [
      {
        hierarchyElement: {
          name: "artist",
          dimensions: ["artists.artist_id", "artists.name"],
          keyDimensions: ["artists.artist_id"],
          formatDimensions: ["artists.name"],
        },
        hierarchyElementFilterDimensions: [],
        query: {
          members: ["artists.artist_id", "invoices.total"],
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
        },
      },
      {
        hierarchyElement: {
          name: "album",
          dimensions: ["albums.album_id", "albums.title"],
          keyDimensions: ["albums.album_id"],
          formatDimensions: ["albums.title"],
        },
        hierarchyElementFilterDimensions: ["artists.artist_id"],
        query: {
          members: ["artists.artist_id", "albums.album_id", "invoices.total"],
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
        },
      },
      {
        hierarchyElement: {
          name: "track",
          dimensions: ["tracks.track_id", "tracks.name"],
          keyDimensions: ["tracks.track_id"],
          formatDimensions: ["tracks.name"],
        },
        hierarchyElementFilterDimensions: [
          "artists.artist_id",
          "albums.album_id",
        ],
        query: {
          members: [
            "artists.artist_id",
            "albums.album_id",
            "tracks.track_id",
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
        },
      },
      {
        hierarchyElement: {
          name: "track",
          dimensions: ["tracks.track_id", "tracks.name"],
          keyDimensions: ["tracks.track_id"],
          formatDimensions: ["tracks.name"],
        },
        hierarchyElementFilterDimensions: [
          "artists.artist_id",
          "albums.album_id",
        ],
        query: {
          members: [
            "artists.artist_id",
            "albums.album_id",
            "tracks.track_id",
            "customers.full_name",
            "customers.email",
            "customers.phone",
            "invoices.invoice_date",
            "tracks.track_id",
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
        },
      },
    ],
  });

  const queriesForHierarchyCustomerPersonalInformation = getQueriesForHierarchy(
    queryAnalysis,
    "customers.personal_information",
  );

  expect(queriesForHierarchyCustomerPersonalInformation).toMatchObject({
    restMembers: [
      "customers.email",
      "customers.phone",
      "artists.name",
      "albums.title",
      "tracks.track_id",
      "invoices.invoice_date",
      "invoices.total",
    ],
    queriesInfo: [
      {
        hierarchyElement: {
          name: "personal_information",
          dimensions: [
            "customers.customer_id",
            "customers.first_name",
            "customers.last_name",
            "customers.full_name",
            "customers.email",
            "customers.fax",
            "customers.phone",
          ],
          keyDimensions: ["customers.customer_id"],
          formatDimensions: ["customers.full_name"],
        },
        hierarchyElementFilterDimensions: [],
        query: {
          members: [
            "customers.customer_id",
            "customers.email",
            "customers.phone",
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
        },
      },
      {
        hierarchyElement: {
          name: "personal_information",
          dimensions: [
            "customers.customer_id",
            "customers.first_name",
            "customers.last_name",
            "customers.full_name",
            "customers.email",
            "customers.fax",
            "customers.phone",
          ],
          keyDimensions: ["customers.customer_id"],
          formatDimensions: ["customers.full_name"],
        },
        hierarchyElementFilterDimensions: [],
        query: {
          members: [
            "customers.customer_id",
            "customers.email",
            "customers.phone",
            "artists.name",
            "albums.title",
            "tracks.track_id",
            "invoices.invoice_date",
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
        },
      },
    ],
  });
});
