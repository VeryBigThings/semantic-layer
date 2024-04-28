import { SqlFragment, SqlQueryBuilder } from "../sql-query-builder.js";

import { BaseDialect } from "../base.js";

export class SqlQuery {
  constructor(
    private readonly dialect: BaseDialect,
    public readonly sql: string,
    public readonly bindings: unknown[],
  ) {}

  toNative() {
    return {
      sql: this.dialect.sqlToNative(this.sql),
      bindings: this.bindings,
    };
  }
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Essential complexity for SQL serialization
export function toSQL(sqlQueryBuilder: SqlQueryBuilder) {
  const sql: string[] = [];
  const bindings: unknown[] = [];

  sql.push("select");

  if (sqlQueryBuilder.query.distinct) {
    sql.push("distinct");
  }

  const sqlSelect: string[] = [];

  for (const select of sqlQueryBuilder.query.select) {
    if (select instanceof SqlFragment) {
      sqlSelect.push(select.sql);
      bindings.push(...select.bindings);
    } else {
      sqlSelect.push(select);
    }
  }

  sql.push(sqlSelect.join(", "));

  sql.push("from");

  if (sqlQueryBuilder.from instanceof SqlQueryBuilder) {
    const { sql: fromSql, bindings: fromBindings } = toSQL(
      sqlQueryBuilder.from,
    );
    sql.push(`(${fromSql})`);
    bindings.push(...fromBindings);
    if (sqlQueryBuilder.from.query.alias) {
      sql.push(
        `as ${sqlQueryBuilder.dialect.asIdentifier(
          sqlQueryBuilder.from.query.alias,
        )}`,
      );
    }
  } else if (sqlQueryBuilder.from instanceof SqlFragment) {
    sql.push(sqlQueryBuilder.from.sql);
    bindings.push(...sqlQueryBuilder.from.bindings);
  } else {
    sql.push(sqlQueryBuilder.from);
  }

  for (const join of sqlQueryBuilder.query.joins) {
    sql.push(`${join.type} join`);
    if (join.table instanceof SqlQueryBuilder) {
      const { sql: joinSql, bindings: joinBindings } = toSQL(join.table);
      sql.push(`(${joinSql})`);
      bindings.push(...joinBindings);
      if (join.table.query.alias) {
        sql.push(`as ${join.table.query.alias}`);
      }
    } else if (join.table instanceof SqlFragment) {
      sql.push(join.table.sql);
      bindings.push(...join.table.bindings);
    } else {
      sql.push(join.table);
    }

    sql.push("on");
    if (join.on instanceof SqlFragment) {
      sql.push(join.on.sql);
      bindings.push(...join.on.bindings);
    } else {
      sql.push(join.on);
    }
  }

  for (const where of sqlQueryBuilder.query.where) {
    sql.push("where");
    if (where instanceof SqlFragment) {
      sql.push(where.sql);
      bindings.push(...where.bindings);
    } else {
      sql.push(where);
    }
  }

  if (sqlQueryBuilder.query.groupBy.length) {
    sql.push("group by");
    for (const groupBy of sqlQueryBuilder.query.groupBy) {
      if (groupBy instanceof SqlFragment) {
        sql.push(groupBy.sql);
        bindings.push(...groupBy.bindings);
      } else {
        sql.push(groupBy);
      }
    }
  }

  if (sqlQueryBuilder.query.orderBy.length) {
    sql.push("order by");
    for (const orderBy of sqlQueryBuilder.query.orderBy) {
      if (orderBy instanceof SqlFragment) {
        sql.push(orderBy.sql);
        bindings.push(...orderBy.bindings);
      } else {
        sql.push(orderBy);
      }
    }
  }

  if (sqlQueryBuilder.query.limit) {
    sql.push("limit ?");
    bindings.push(sqlQueryBuilder.query.limit);
  }

  if (sqlQueryBuilder.query.offset) {
    sql.push("offset ?");
    bindings.push(sqlQueryBuilder.query.offset);
  }

  return new SqlQuery(sqlQueryBuilder.dialect, sql.join(" "), bindings);
}
