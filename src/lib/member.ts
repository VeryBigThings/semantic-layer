import { MemberFormat, MemberType } from "./types.js";

import { AnyBaseDialect } from "./dialect/base.js";
import { GranularityDimension } from "./model/granularity-dimension.js";
import { QueryContext } from "./query-builder/query-plan/query-context.js";
import { QueryMember } from "./query-builder/query-plan/query-member.js";
import { AnyRepository } from "./repository.js";

export abstract class Member {
  abstract isMetric(): this is Metric;
  abstract isDimension(): this is Dimension;

  abstract isPrivate(): boolean;

  abstract getAlias(): string;
  abstract getPath(): string;
  abstract getDescription(): string | undefined;
  abstract getType(): MemberType;
  abstract getFormat(): MemberFormat | undefined;
  abstract getQueryMember(
    queryContext: QueryContext,
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ): QueryMember;

  unsafeFormatValue(value: unknown) {
    const format = this.getFormat();
    if (typeof format === "function") {
      return (format as (value: unknown) => string)(value);
    }
    if (format === "currency") {
      return `$${value}`;
    }
    if (format === "percentage") {
      return `${value}%`;
    }
    return String(value);
  }
}

export abstract class Dimension extends Member {
  abstract isPrimaryKey(): boolean;
  abstract isGranularity(): this is GranularityDimension;
}
export abstract class Metric extends Member {}
