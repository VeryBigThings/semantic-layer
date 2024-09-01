import {
  ColumnRef,
  DimensionRef,
  MetricAliasColumnOrDimensionRef,
  MetricAliasMetricRef,
  SqlFn,
  valueIsMetricAliasRef,
} from "../../sql-fn.js";

import { AnyBaseDialect } from "../../dialect/base.js";
import { Member } from "../../member.js";
import { AnyRepository } from "../../repository.js";
import { SqlFragment } from "../../sql-builder.js";
import { QueryContext } from "./query-context.js";

export abstract class QueryMember {
  abstract readonly queryContext: QueryContext;
  abstract readonly repository: AnyRepository;
  abstract readonly dialect: AnyBaseDialect;
  abstract readonly context: unknown;
  abstract readonly member: Member;

  abstract getSql(): SqlFragment;
  abstract getFilterSql(): SqlFragment;
  abstract getAlias(): string;
  abstract getModelQueryProjection(): SqlFragment[];
  abstract getSegmentQueryProjection(modelQueryAlias: string): SqlFragment[];
  abstract getSegmentQueryGroupBy(modelQueryAlias: string): SqlFragment[];
  abstract getRootQueryProjection(segmentQueryAlias: string): SqlFragment[];
  abstract getReferencedModels(): string[];
}

export abstract class DimensionQueryMember extends QueryMember {
  getAlias() {
    return this.member.getAlias();
  }
  getFilterSql() {
    return this.getSql();
  }
  getModelQueryProjection() {
    const { sql, bindings } = this.getSql();
    const fragment = this.dialect.fragment(
      `${sql} as ${this.dialect.asIdentifier(this.member.getAlias())}`,
      bindings,
    );
    return [fragment];
  }
  getSegmentQueryProjection(modelQueryAlias: string) {
    const fragment = this.dialect.fragment(
      `${this.dialect.asIdentifier(modelQueryAlias)}.${this.dialect.asIdentifier(
        this.member.getAlias(),
      )} as ${this.dialect.asIdentifier(this.member.getAlias())}`,
    );
    return [fragment];
  }
  getSegmentQueryGroupBy(modelQueryAlias: string) {
    const fragment = this.dialect.fragment(
      `${this.dialect.asIdentifier(modelQueryAlias)}.${this.dialect.asIdentifier(
        this.member.getAlias(),
      )}`,
    );
    return [fragment];
  }
  getRootQueryProjection(segmentQueryAlias: string) {
    const fragment = this.dialect.fragment(
      `${this.dialect.asIdentifier(segmentQueryAlias)}.${this.dialect.asIdentifier(
        this.member.getAlias(),
      )} as ${this.dialect.asIdentifier(this.member.getAlias())}`,
    );
    return [fragment];
  }
}

export abstract class MetricQueryMember extends QueryMember {
  abstract readonly sqlFnResult: SqlFn;
  abstract readonly sqlFnRenderResult: SqlFragment;
  getAlias() {
    return this.member.getAlias();
  }
  getSql() {
    return this.sqlFnRenderResult;
  }
  getFilterSql() {
    return SqlFragment.fromSql(this.dialect.asIdentifier(this.getAlias()));
  }
  getModelQueryProjection() {
    const sqlFnResult = this.sqlFnResult;
    return sqlFnResult
      .filterRefs(valueIsMetricAliasRef)
      .map(({ alias, aliasOf }) => {
        const { sql, bindings } = aliasOf.render(
          this.repository,
          this.queryContext,
          this.dialect,
        );
        return SqlFragment.make({
          sql: `${sql} as ${this.dialect.asIdentifier(alias)}`,
          bindings,
        });
      });
  }
  getSegmentQueryProjection(_modelQueryAlias: string) {
    const { sql, bindings } = this.getSql();
    const fragment = this.dialect.fragment(
      `${sql} as ${this.dialect.asIdentifier(this.member.getAlias())}`,
      bindings,
    );
    return [fragment];
  }
  getSegmentQueryGroupBy(modelQueryAlias: string) {
    return this.sqlFnResult
      .filterRefs(valueIsMetricAliasRef)
      .filter(
        (ref) =>
          (ref instanceof MetricAliasColumnOrDimensionRef &&
            ref.getIsGroupedBy()) ||
          (ref instanceof MetricAliasMetricRef && ref.getIsAggregated()),
      )
      .map((ref) =>
        this.dialect.fragment(
          `${this.dialect.asIdentifier(modelQueryAlias)}.${this.dialect.asIdentifier(
            ref.alias,
          )}`,
        ),
      );
  }
  getRootQueryProjection(segmentQueryAlias: string) {
    const fragment = this.dialect.fragment(
      `${this.dialect.asIdentifier(segmentQueryAlias)}.${this.dialect.asIdentifier(
        this.member.getAlias(),
      )} as ${this.dialect.asIdentifier(this.member.getAlias())}`,
    );
    return [fragment];
  }
  getMetricRefs() {
    const filterFn = (ref: unknown): ref is MetricAliasMetricRef =>
      ref instanceof MetricAliasMetricRef;
    return this.sqlFnResult.filterRefs(filterFn).map((v) => v.aliasOf);
  }

  getReferencedModels() {
    const referencedModels = this.sqlFnResult
      .filterRefs(valueIsMetricAliasRef)
      .flatMap((aliasRef) => {
        const { aliasOf } = aliasRef;
        if (aliasOf instanceof ColumnRef) {
          return [aliasOf.model.name];
        }
        if (aliasOf instanceof DimensionRef) {
          const dimension = aliasOf.member;
          const dimensionQueryMember =
            this.queryContext.getQueryMember(dimension);
          return dimensionQueryMember.getReferencedModels();
        }
        if (
          aliasRef instanceof MetricAliasMetricRef &&
          !aliasRef.getIsAggregated()
        ) {
          const metric = aliasRef.aliasOf.member;
          const metricQueryMember = this.queryContext.getQueryMember(metric);
          return metricQueryMember.getReferencedModels();
        }
        return [];
      });
    return Array.from(new Set(referencedModels));
  }
}
