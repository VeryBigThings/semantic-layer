import { SqlQuery, toSQL } from "./sql-query-builder/to-sql.js";

import { BaseDialect } from "./base.js";

export interface QueryJoin {
  table: string | SqlFragment | SqlQueryBuilder;
  on: string | SqlFragment;
  type: "inner" | "left" | "right";
}

export interface SqlQueryStructure {
  distinct: boolean;
  alias: null | string;
  select: (string | SqlFragment)[];
  where: (string | SqlFragment)[];
  orderBy: (string | SqlFragment)[];
  limit: null | number;
  offset: null | number;
  groupBy: (string | SqlFragment)[];
  joins: QueryJoin[];
}

export type From = string | SqlQueryBuilder | SqlFragment;

export class SqlQueryBuilder {
  public query: SqlQueryStructure = {
    distinct: false,
    select: [],
    alias: null,
    where: [],
    orderBy: [],
    limit: null,
    offset: null,
    groupBy: [],
    joins: [],
  };
  constructor(
    public readonly dialect: BaseDialect,
    public readonly from: From,
  ) {}

  distinct() {
    this.query = { ...this.query, distinct: true };
    return this;
  }

  as(alias: string) {
    this.query = { ...this.query, alias: alias };
    return this;
  }

  select(select: string | SqlFragment) {
    this.query = { ...this.query, select: [...this.query.select, select] };
    return this;
  }

  where(where: string | SqlFragment) {
    this.query = { ...this.query, where: [...this.query.where, where] };
    return this;
  }

  orderBy(orderBy: string | SqlFragment) {
    this.query = { ...this.query, orderBy: [...this.query.orderBy, orderBy] };
    return this;
  }

  limit(limit: number) {
    this.query = { ...this.query, limit: limit };
    return this;
  }

  offset(offset: number) {
    this.query = { ...this.query, offset: offset };
    return this;
  }

  groupBy(groupBy: string | SqlFragment) {
    this.query = { ...this.query, groupBy: [...this.query.groupBy, groupBy] };
    return this;
  }

  join(
    table: string | SqlFragment | SqlQueryBuilder,
    on: string | SqlFragment,
    type: "inner" | "left" | "right",
  ) {
    this.query = {
      ...this.query,
      joins: [...this.query.joins, { table, on, type }],
    };
    return this;
  }

  leftJoin(
    table: string | SqlFragment | SqlQueryBuilder,
    on: string | SqlFragment,
  ) {
    return this.join(table, on, "left");
  }

  rightJoin(
    table: string | SqlFragment | SqlQueryBuilder,
    on: string | SqlFragment,
  ) {
    return this.join(table, on, "right");
  }

  innerJoin(
    table: string | SqlFragment | SqlQueryBuilder,
    on: string | SqlFragment,
  ) {
    return this.join(table, on, "inner");
  }

  toSQL(): SqlQuery {
    return toSQL(this);
  }
}

export class SqlFragment {
  constructor(
    public sql: string,
    public bindings: unknown[] = [],
  ) {}
}
