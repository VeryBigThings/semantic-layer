import { SqlWithBindings } from "../types.js";

export function sqlAsSqlWithBindings(sql: string): SqlWithBindings {
  return { sql, bindings: [] };
}
