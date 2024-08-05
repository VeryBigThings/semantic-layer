import * as semanticLayer from "../index.js";

import { assert, it } from "vitest";

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
  .withGranularity("address", [
    "country",
    "state",
    "city",
    "postal_code",
    "address",
  ])
  .withGranularity("personal_data", [
    {
      key: "personal_data",
      elements: ["first_name", "last_name", "full_name", "customer_id"],
      display: "full_name",
    },
  ])
  .withGranularity("company", ["company"]);

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
    sql: ({ model, sql }) => sql`SUM(COALESCE, ${model.column("Total")}, 0))`,
  })
  .withGranularity("billing_address", [
    "billing_country",
    "billing_state",
    "billing_city",
    "billing_postal_code",
    "billing_address",
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
  })
  .withGranularity("name", [
    {
      key: "name",
      elements: ["name", "artist_id"],
      display: "name",
    },
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
  .withGranularity("name", [
    {
      key: "name",
      elements: ["name", "media_type_id"],
      display: "name",
    },
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
  .withGranularity("name", [
    {
      key: "data",
      elements: ["name", "genre_id"],
      display: "name",
    },
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
  .withGranularity("name", [
    {
      key: "name",
      elements: ["name", "playlist_id"],
      display: "name",
    },
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
  .withGranularity("album", [
    "artists.name",
    {
      key: "album.data",
      elements: ["albums.title", "albums.album_id"],
      display: "albums.title",
    },
  ])
  .withGranularity("track", [
    "artists.name",
    "albums.title",
    {
      key: "track.data",
      elements: ["tracks.name", "tracks.track_id"],
      display: "tracks.name",
    },
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

//const queryBuilder = repository.build("postgresql");

it("can correctly generate granularities", () => {
  assert.deepEqual(repository.granularities, [
    {
      name: "track",
      type: "custom",
      elements: [
        "artists.name",
        "albums.title",
        {
          key: "track.data",
          elements: ["tracks.name", "tracks.track_id"],
          display: "tracks.name",
        },
      ],
    },
    {
      name: "album",
      type: "custom",
      elements: [
        "artists.name",
        {
          key: "album.data",
          elements: ["albums.title", "albums.album_id"],
          display: "albums.title",
        },
      ],
    },
    {
      name: "customers.company",
      type: "custom",
      elements: ["customers.company"],
    },
    {
      name: "customers.personal_data",
      type: "custom",
      elements: [
        {
          key: "personal_data",
          elements: [
            "customers.first_name",
            "customers.last_name",
            "customers.full_name",
            "customers.customer_id",
          ],
          display: "customers.full_name",
        },
      ],
    },
    {
      name: "customers.address",
      type: "custom",
      elements: [
        "customers.country",
        "customers.state",
        "customers.city",
        "customers.postal_code",
        "customers.address",
      ],
    },
    {
      name: "invoices.billing_address",
      type: "custom",
      elements: [
        "invoices.billing_country",
        "invoices.billing_state",
        "invoices.billing_city",
        "invoices.billing_postal_code",
        "invoices.billing_address",
      ],
    },
    {
      name: "invoices.invoice_date",
      type: "temporal",
      elements: [
        "invoices.invoice_date.year",
        "invoices.invoice_date.quarter",
        "invoices.invoice_date.quarter_of_year",
        "invoices.invoice_date.month",
        "invoices.invoice_date.month_num",
        "invoices.invoice_date.week",
        "invoices.invoice_date.week_num",
        "invoices.invoice_date.day_of_month",
        "invoices.invoice_date",
      ],
    },
    {
      name: "artists.name",
      type: "custom",
      elements: [
        {
          key: "name",
          elements: ["artists.name", "artists.artist_id"],
          display: "artists.name",
        },
      ],
    },
    {
      name: "media_types.name",
      type: "custom",
      elements: [
        {
          key: "name",
          elements: ["media_types.name", "media_types.media_type_id"],
          display: "media_types.name",
        },
      ],
    },
    {
      name: "genres.name",
      type: "custom",
      elements: [
        {
          key: "data",
          elements: ["genres.name", "genres.genre_id"],
          display: "genres.name",
        },
      ],
    },
    {
      name: "playlists.name",
      type: "custom",
      elements: [
        {
          key: "name",
          elements: ["playlists.name", "playlists.playlist_id"],
          display: "playlists.name",
        },
      ],
    },
  ]);
});