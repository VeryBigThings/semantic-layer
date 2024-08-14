import { AnsiDialect } from "./ansi.js";

export class PostgreSQLDialect extends AnsiDialect {
  sqlToNative(sql: string) {
    return this.positionBindings(sql);
  }
  private positionBindings(sql: string) {
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
