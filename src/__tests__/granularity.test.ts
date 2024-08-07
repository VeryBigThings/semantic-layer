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
  .withCategoricalGranularity("full_address", ({ element }) => [
    element.fromDimension("country"),
    element.fromDimension("state"),
    element.fromDimension("city"),
    element.fromDimension("postal_code"),
    element.fromDimension("address"),
  ])

  .withCategoricalGranularity("personal_information", ({ element }) => [
    element("customer")
      .withDimensions(["customer_id", "first_name", "last_name"])
      .withKey(["customer_id"])
      .withFormat(
        ["first_name", "last_name"],
        ({ dimension }) =>
          `${dimension("first_name")} ${dimension("last_name")}`,
      ),
  ])
  .withCategoricalGranularity("company", ({ element }) => [
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
    sql: ({ model, sql }) => sql`SUM(COALESCE, ${model.column("Total")}, 0))`,
  })
  .withCategoricalGranularity("billing_address", ({ element }) => [
    element.fromDimension("billing_country"),
    element.fromDimension("billing_state"),
    element.fromDimension("billing_city"),
    element.fromDimension("billing_postal_code"),
    element.fromDimension("billing_address"),
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
  .withCategoricalGranularity("artist", ({ element }) => [
    element("name")
      .withDimensions(["name", "artist_id"])
      .withKey(["artist_id"])
      .withFormat(["name"], ({ dimension }) => `${dimension("name")}`),
  ])
  .withCategoricalGranularity("formatting_test", ({ element }) => [
    element("name1").withDimensions(["name", "artist_id"]),
    element("name2")
      .withDimensions(["name", "artist_id"])
      .withFormat(["artist_id", "name"]),
    element("name3")
      .withDimensions(["name", "artist_id"])
      .withFormat(
        ["name", "artist_id"],
        ({ dimension }) =>
          `ID: ${dimension("artist_id").originalValue}, Artist Name: ${
            dimension("name").originalValue
          }`,
      ),
    element("name4")
      .withDimensions(["name", "artist_id"])
      .withFormat(
        ["name", "artist_id"],
        ({ dimension }) =>
          `${dimension("artist_id").formattedValue} ${
            dimension("name").formattedValue
          }`,
      ),
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
  .withCategoricalGranularity("media_type", ({ element }) => [
    element("name")
      .withDimensions(["name", "media_type_id"])
      .withFormat(["name"], ({ dimension }) => `${dimension("name")}`),
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
  .withCategoricalGranularity("genre", ({ element }) => [
    element("name")
      .withDimensions(["name", "genre_id"])
      .withFormat(["name"], ({ dimension }) => `${dimension("name")}`),
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
  .withCategoricalGranularity("name", ({ element }) => [
    element("name")
      .withDimensions(["playlist_id", "name"])
      .withKey(["playlist_id"])
      .withFormat(["name"], ({ dimension }) => `${dimension("name")}`),
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
  .withCategoricalGranularity("album", ({ element }) => [
    element("artists.name")
      .withDimensions(["artists.name", "artists.artist_id"])
      .withKey(["artists.artist_id"])
      .withFormat(
        ["artists.name"],
        ({ dimension }) => `${dimension("artists.name")}`,
      ),
    element("album.title")
      .withDimensions(["albums.title", "albums.album_id"])
      .withKey(["albums.album_id"])
      .withFormat(
        ["albums.title"],
        ({ dimension }) => `${dimension("albums.title")}`,
      ),
  ])
  .withCategoricalGranularity("track", ({ element }) => [
    // Reduce this duplication by tracking element names in the generic, and then using them here by adding a function that will look like this: granularity("album").element("artists.name")
    element("artists.name")
      .withDimensions(["artists.name", "artists.artist_id"])
      .withKey(["artists.artist_id"])
      .withFormat(
        ["artists.name"],
        ({ dimension }) => `${dimension("artists.name")}`,
      ),
    element("album.title")
      .withDimensions(["albums.title", "albums.album_id"])
      .withKey(["albums.album_id"])
      .withFormat(
        ["albums.title"],
        ({ dimension }) => `${dimension("albums.title")}`,
      ),
    element("track.name")
      .withDimensions(["tracks.name", "tracks.track_id"])
      .withKey(["tracks.track_id"])
      .withFormat(
        ["tracks.name"],
        ({ dimension }) => `${dimension("tracks.name")}`,
      ),
  ])
  .withCategoricalGranularity("formatting_test", ({ element }) => [
    element("name1").withDimensions(["artists.name", "artists.artist_id"]),
    element("name2")
      .withDimensions(["artists.name", "artists.artist_id"])
      .withFormat(["artists.artist_id", "artists.name"]),
    element("name3")
      .withDimensions(["artists.name", "artists.artist_id"])
      .withFormat(
        ["artists.name", "artists.artist_id"],
        ({ dimension }) =>
          `ID: ${dimension("artists.artist_id").originalValue}, Artist Name: ${
            dimension("artists.name").originalValue
          }`,
      ),
    element("name4")
      .withDimensions(["artists.name", "artists.artist_id"])
      .withFormat(
        ["artists.name", "artists.artist_id"],
        ({ dimension }) =>
          `${dimension("artists.artist_id").formattedValue} ${
            dimension("artists.name").formattedValue
          }`,
      ),
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

const queryBuilder = repository.build("postgresql");

it("can correctly generate granularities", () => {
  const granularitiesWithoutFormatters = queryBuilder.granularities.map(
    (granularity) => {
      const elements = granularity.elements.map(
        ({ formatter: _formatter, ...element }) => {
          return element;
        },
      );
      return {
        ...granularity,
        elements: elements,
      };
    },
  );

  for (const granularity of queryBuilder.granularities) {
    for (const element of granularity.elements) {
      assert.isFunction(element.formatter);
    }
  }

  assert.deepEqual(granularitiesWithoutFormatters, [
    {
      name: "album",
      type: "categorical",
      elements: [
        {
          name: "artists.name",
          dimensions: ["artists.name", "artists.artist_id"],
          keyDimensions: ["artists.artist_id"],
          formatDimensions: ["artists.name"],
        },
        {
          name: "album.title",
          dimensions: ["albums.title", "albums.album_id"],
          keyDimensions: ["albums.album_id"],
          formatDimensions: ["albums.title"],
        },
      ],
    },
    {
      name: "track",
      type: "categorical",
      elements: [
        {
          name: "artists.name",
          dimensions: ["artists.name", "artists.artist_id"],
          keyDimensions: ["artists.artist_id"],
          formatDimensions: ["artists.name"],
        },
        {
          name: "album.title",
          dimensions: ["albums.title", "albums.album_id"],
          keyDimensions: ["albums.album_id"],
          formatDimensions: ["albums.title"],
        },
        {
          name: "track.name",
          dimensions: ["tracks.name", "tracks.track_id"],
          keyDimensions: ["tracks.track_id"],
          formatDimensions: ["tracks.name"],
        },
      ],
    },
    {
      name: "formatting_test",
      type: "categorical",
      elements: [
        {
          name: "name1",
          dimensions: ["artists.name", "artists.artist_id"],
          keyDimensions: ["artists.name", "artists.artist_id"],
          formatDimensions: ["artists.name", "artists.artist_id"],
        },
        {
          name: "name2",
          dimensions: ["artists.name", "artists.artist_id"],
          keyDimensions: ["artists.name", "artists.artist_id"],
          formatDimensions: ["artists.artist_id", "artists.name"],
        },
        {
          name: "name3",
          dimensions: ["artists.name", "artists.artist_id"],
          keyDimensions: ["artists.name", "artists.artist_id"],
          formatDimensions: ["artists.name", "artists.artist_id"],
        },
        {
          name: "name4",
          dimensions: ["artists.name", "artists.artist_id"],
          keyDimensions: ["artists.name", "artists.artist_id"],
          formatDimensions: ["artists.name", "artists.artist_id"],
        },
      ],
    },
    {
      name: "customers.full_address",
      type: "categorical",
      elements: [
        {
          name: "country",
          dimensions: ["customers.country"],
          keyDimensions: ["customers.country"],
          formatDimensions: ["customers.country"],
        },
        {
          name: "state",
          dimensions: ["customers.state"],
          keyDimensions: ["customers.state"],
          formatDimensions: ["customers.state"],
        },
        {
          name: "city",
          dimensions: ["customers.city"],
          keyDimensions: ["customers.city"],
          formatDimensions: ["customers.city"],
        },
        {
          name: "postal_code",
          dimensions: ["customers.postal_code"],
          keyDimensions: ["customers.postal_code"],
          formatDimensions: ["customers.postal_code"],
        },
        {
          name: "address",
          dimensions: ["customers.address"],
          keyDimensions: ["customers.address"],
          formatDimensions: ["customers.address"],
        },
      ],
    },
    {
      name: "customers.personal_information",
      type: "categorical",
      elements: [
        {
          name: "customer",
          dimensions: [
            "customers.customer_id",
            "customers.first_name",
            "customers.last_name",
          ],
          keyDimensions: ["customers.customer_id"],
          formatDimensions: ["customers.first_name", "customers.last_name"],
        },
      ],
    },
    {
      name: "customers.company",
      type: "categorical",
      elements: [
        {
          name: "company",
          dimensions: ["customers.company"],
          keyDimensions: ["customers.company"],
          formatDimensions: ["customers.company"],
        },
      ],
    },
    {
      name: "invoices.billing_address",
      type: "categorical",
      elements: [
        {
          name: "billing_country",
          dimensions: ["invoices.billing_country"],
          keyDimensions: ["invoices.billing_country"],
          formatDimensions: ["invoices.billing_country"],
        },
        {
          name: "billing_state",
          dimensions: ["invoices.billing_state"],
          keyDimensions: ["invoices.billing_state"],
          formatDimensions: ["invoices.billing_state"],
        },
        {
          name: "billing_city",
          dimensions: ["invoices.billing_city"],
          keyDimensions: ["invoices.billing_city"],
          formatDimensions: ["invoices.billing_city"],
        },
        {
          name: "billing_postal_code",
          dimensions: ["invoices.billing_postal_code"],
          keyDimensions: ["invoices.billing_postal_code"],
          formatDimensions: ["invoices.billing_postal_code"],
        },
        {
          name: "billing_address",
          dimensions: ["invoices.billing_address"],
          keyDimensions: ["invoices.billing_address"],
          formatDimensions: ["invoices.billing_address"],
        },
      ],
    },
    {
      name: "invoices.invoice_date",
      type: "temporal",
      elements: [
        {
          name: "invoice_date.year",
          dimensions: ["invoices.invoice_date.year"],
          keyDimensions: ["invoices.invoice_date.year"],
          formatDimensions: ["invoices.invoice_date.year"],
        },
        {
          name: "invoice_date.quarter",
          dimensions: ["invoices.invoice_date.quarter"],
          keyDimensions: ["invoices.invoice_date.quarter"],
          formatDimensions: ["invoices.invoice_date.quarter"],
        },
        {
          name: "invoice_date.quarter_of_year",
          dimensions: ["invoices.invoice_date.quarter_of_year"],
          keyDimensions: ["invoices.invoice_date.quarter_of_year"],
          formatDimensions: ["invoices.invoice_date.quarter_of_year"],
        },
        {
          name: "invoice_date.month",
          dimensions: ["invoices.invoice_date.month"],
          keyDimensions: ["invoices.invoice_date.month"],
          formatDimensions: ["invoices.invoice_date.month"],
        },
        {
          name: "invoice_date.month_num",
          dimensions: ["invoices.invoice_date.month_num"],
          keyDimensions: ["invoices.invoice_date.month_num"],
          formatDimensions: ["invoices.invoice_date.month_num"],
        },
        {
          name: "invoice_date.week",
          dimensions: ["invoices.invoice_date.week"],
          keyDimensions: ["invoices.invoice_date.week"],
          formatDimensions: ["invoices.invoice_date.week"],
        },
        {
          name: "invoice_date.week_num",
          dimensions: ["invoices.invoice_date.week_num"],
          keyDimensions: ["invoices.invoice_date.week_num"],
          formatDimensions: ["invoices.invoice_date.week_num"],
        },
        {
          name: "invoice_date.day_of_month",
          dimensions: ["invoices.invoice_date.day_of_month"],
          keyDimensions: ["invoices.invoice_date.day_of_month"],
          formatDimensions: ["invoices.invoice_date.day_of_month"],
        },
        {
          name: "invoice_date",
          dimensions: ["invoices.invoice_date"],
          keyDimensions: ["invoices.invoice_date"],
          formatDimensions: ["invoices.invoice_date"],
        },
      ],
    },
    {
      name: "artists.artist",
      type: "categorical",
      elements: [
        {
          name: "name",
          dimensions: ["artists.name", "artists.artist_id"],
          keyDimensions: ["artists.artist_id"],
          formatDimensions: ["artists.name"],
        },
      ],
    },
    {
      name: "artists.formatting_test",
      type: "categorical",
      elements: [
        {
          name: "name1",
          dimensions: ["artists.name", "artists.artist_id"],
          keyDimensions: ["artists.name", "artists.artist_id"],
          formatDimensions: ["artists.name", "artists.artist_id"],
        },
        {
          name: "name2",
          dimensions: ["artists.name", "artists.artist_id"],
          keyDimensions: ["artists.name", "artists.artist_id"],
          formatDimensions: ["artists.artist_id", "artists.name"],
        },
        {
          name: "name3",
          dimensions: ["artists.name", "artists.artist_id"],
          keyDimensions: ["artists.name", "artists.artist_id"],
          formatDimensions: ["artists.name", "artists.artist_id"],
        },
        {
          name: "name4",
          dimensions: ["artists.name", "artists.artist_id"],
          keyDimensions: ["artists.name", "artists.artist_id"],
          formatDimensions: ["artists.name", "artists.artist_id"],
        },
      ],
    },
    {
      name: "media_types.media_type",
      type: "categorical",
      elements: [
        {
          name: "name",
          dimensions: ["media_types.name", "media_types.media_type_id"],
          keyDimensions: ["media_types.name", "media_types.media_type_id"],
          formatDimensions: ["media_types.name"],
        },
      ],
    },
    {
      name: "genres.genre",
      type: "categorical",
      elements: [
        {
          name: "name",
          dimensions: ["genres.name", "genres.genre_id"],
          keyDimensions: ["genres.name", "genres.genre_id"],
          formatDimensions: ["genres.name"],
        },
      ],
    },
    {
      name: "playlists.name",
      type: "categorical",
      elements: [
        {
          name: "name",
          dimensions: ["playlists.playlist_id", "playlists.name"],
          keyDimensions: ["playlists.playlist_id"],
          formatDimensions: ["playlists.name"],
        },
      ],
    },
  ]);
});

it("can correctly format granularities", () => {
  const row = {
    artists___artist_id: 1,
    artists___name: "AC/DC",
  };
  const granularity1 = queryBuilder.getGranularity("artists.formatting_test");
  const formattedValues1 = granularity1.elements.map((element) => [
    element.name,
    element.formatter(row),
  ]);

  const granularity2 = queryBuilder.getGranularity("formatting_test");
  const formattedValues2 = granularity2.elements.map((element) => [
    element.name,
    element.formatter(row),
  ]);

  const expectedValues = [
    ["name1", "Artist: AC/DC, 1"],
    ["name2", "1, Artist: AC/DC"],
    ["name3", "ID: 1, Artist Name: AC/DC"],
    ["name4", "null Artist: AC/DC"],
  ];

  assert.deepEqual(formattedValues1, expectedValues);
  assert.deepEqual(formattedValues2, expectedValues);
});
