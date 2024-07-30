import { AnsiDialect } from "./dialect/ansi.js";
import { DatabricksDialect } from "./dialect/databricks.js";
import { MSSQLDialect } from "./dialect/mssql.js";
import { PostgreSQLDialect } from "./dialect/postgresql.js";

export { AnsiDialect } from "./dialect/ansi.js";
export { DatabricksDialect } from "./dialect/databricks.js";
export { MSSQLDialect } from "./dialect/mssql.js";
export { PostgreSQLDialect } from "./dialect/postgresql.js";

export const AvailableDialects = {
  ansi: new AnsiDialect(),
  postgresql: new PostgreSQLDialect(),
  databricks: new DatabricksDialect(),
  mssql: new MSSQLDialect(),
} as const;

export type AvailableDialects = typeof AvailableDialects;
export type AvailableDialectsNames = keyof AvailableDialects;

export type DialectParamsReturnType<T extends AvailableDialectsNames> =
  ReturnType<AvailableDialects[T]["paramsToNative"]>;
