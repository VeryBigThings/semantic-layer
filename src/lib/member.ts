import {
  QueryMember,
  QueryMemberCache,
} from "./query-builder/query-plan/query-member.js";

import { AnyBaseDialect } from "./dialect/base.js";
import { pathToAlias } from "./helpers.js";
import { AnyModel } from "./model.js";
import { AnyBasicDimensionProps } from "./model/basic-dimension.js";
import { AnyBasicMetricProps } from "./model/basic-metric.js";
import { GranularityDimension } from "./model/granularity-dimension.js";
import { AnyRepository } from "./repository.js";

export abstract class Member {
  public abstract readonly name: string;
  public abstract readonly model: AnyModel;
  public abstract props: AnyBasicDimensionProps | AnyBasicMetricProps;

  abstract isMetric(): this is Metric;
  abstract isDimension(): this is Dimension;

  getAlias() {
    return `${this.model.name}___${pathToAlias(this.name)}`;
  }
  getPath() {
    return `${this.model.name}.${this.name}`;
  }
  getDescription() {
    return this.props.description;
  }
  getType() {
    return this.props.type;
  }
  getFormat() {
    return this.props.format;
  }
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
  abstract clone(model: AnyModel): Member;
  abstract getQueryMember(
    queryMembers: QueryMemberCache,
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ): QueryMember;
}

export abstract class Dimension extends Member {
  abstract isPrimaryKey(): boolean;
  abstract isGranularity(): this is GranularityDimension;
}
export abstract class Metric extends Member {}
