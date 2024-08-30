import {
  AnyBasicDimensionProps,
  BasicDimension,
  BasicDimensionQueryMember,
} from "./basic-dimension.js";

import { AnyBaseDialect } from "../dialect/base.js";
import { AnyModel } from "../model.js";
import { QueryMemberCache } from "../query-builder/query-plan/query-member.js";
import { AnyRepository } from "../repository.js";
import { SqlFragment } from "../sql-builder.js";
import { TemporalGranularity } from "../types.js";

export class GranularityDimension extends BasicDimension {
  constructor(
    model: AnyModel,
    public readonly parent: BasicDimension,
    name: string,
    props: AnyBasicDimensionProps,
    public readonly granularity: TemporalGranularity,
  ) {
    super(model, name, props);
  }
  isGranularity() {
    return true;
  }
  getQueryMember(
    queryMembers: QueryMemberCache,
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ): GranularityDimensionQueryMember {
    return new GranularityDimensionQueryMember(
      queryMembers,
      repository,
      dialect,
      context,
      this,
    );
  }
}

export class GranularityDimensionQueryMember extends BasicDimensionQueryMember {
  constructor(
    queryMembers: QueryMemberCache,
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
    readonly member: GranularityDimension,
  ) {
    super(queryMembers, repository, dialect, context, member);
  }
  getSql() {
    const parent = this.queryMembers.get(this.member.parent);
    const result = parent.getSql();
    return SqlFragment.make({
      sql: this.dialect.withGranularity(this.member.granularity, result.sql),
      bindings: result.bindings,
    });
  }
}
