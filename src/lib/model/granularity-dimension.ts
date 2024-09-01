import {
  AnyBasicDimensionProps,
  BasicDimension,
  BasicDimensionQueryMember,
} from "./basic-dimension.js";

import { AnyBaseDialect } from "../dialect/base.js";
import { AnyModel } from "../model.js";
import { QueryContext } from "../query-builder/query-plan/query-context.js";
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
    queryContext: QueryContext,
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ): GranularityDimensionQueryMember {
    return new GranularityDimensionQueryMember(
      queryContext,
      repository,
      dialect,
      context,
      this,
    );
  }
}

export class GranularityDimensionQueryMember extends BasicDimensionQueryMember {
  constructor(
    queryContext: QueryContext,
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
    readonly member: GranularityDimension,
  ) {
    super(queryContext, repository, dialect, context, member);
  }
  getSql() {
    const parent = this.queryContext.getQueryMember(this.member.parent);
    const result = parent.getSql();
    return SqlFragment.make({
      sql: this.dialect.withGranularity(this.member.granularity, result.sql),
      bindings: result.bindings,
    });
  }
}
