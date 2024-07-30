import { SqlWithBindings } from "../types.js";

export function sqlAsSqlWithBindings(sql: string): SqlWithBindings {
  return { sql, bindings: [] };
}

export function exhaustiveCheck(
  _exhaustiveCheck: never,
  message: string,
): never {
  throw new Error(message);
}
