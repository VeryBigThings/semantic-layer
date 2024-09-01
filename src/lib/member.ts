import {
  QueryMember,
  QueryMemberCache,
} from "./query-builder/query-plan/query-member.js";
import { MemberFormat, MemberType } from "./types.js";

import { AnyBaseDialect } from "./dialect/base.js";
import { GranularityDimension } from "./model/granularity-dimension.js";
import { AnyRepository } from "./repository.js";

export abstract class Member {
  abstract isMetric(): this is Metric;
  abstract isDimension(): this is Dimension;

  abstract getAlias(): string;
  abstract getPath(): string;
  abstract getDescription(): string | undefined;
  abstract getType(): MemberType;
  abstract getFormat(): MemberFormat | undefined;
  abstract getQueryMember(
    queryMembers: QueryMemberCache,
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
