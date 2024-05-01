import { AnsiDialect } from "./ansi.js";

export class PostgresqlDialect extends AnsiDialect {
  sqlToNative(sql: string) {
    return this.positionBindings(sql);
  }
  positionBindings(sql: string) {
    let questionCount = 0;
    return sql.replace(/(\\*)(\?)/g, (_match, escapes) => {
      if (escapes.length % 2) {
        return "?";
      }
      questionCount++;
      return `$${questionCount}`;
    });
  }
}
