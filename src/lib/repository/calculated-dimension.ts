import {
  QueryMember,
  QueryMemberCache,
} from "../query-builder/query-plan/query-member.js";
import { ColumnRef, DimensionRef, IdentifierRef, SqlFn } from "../sql-fn.js";
import { MemberProps, ModelMemberWithoutModelPrefix } from "../types.js";

import invariant from "tiny-invariant";
import { AnyBaseDialect } from "../dialect/base.js";
import { pathToAlias } from "../helpers.js";
import { Dimension } from "../member.js";
import { AnyRepository } from "../repository.js";
import { SqlFragment } from "../sql-builder.js";
import { isNonEmptyArray } from "../util.js";

export type CalculateDimensionSqlFnArgsModels<
  TModelNames extends string,
  TDimensionNames extends string,
> = {
  [TK in TModelNames]: {
    dimension: (
      dimensionName: ModelMemberWithoutModelPrefix<TK, TDimensionNames>,
    ) => DimensionRef;
    column: (columnName: string) => ColumnRef;
  };
};

export interface CalculatedDimensionSqlFnArgs<
  TContext,
  TModelNames extends string,
  TDimensionNames extends string,
> {
  identifier: (name: string) => IdentifierRef;
  models: CalculateDimensionSqlFnArgsModels<TModelNames, TDimensionNames>;
  sql: (strings: TemplateStringsArray, ...values: unknown[]) => SqlFn;
  getContext: () => TContext;
}

export type CalculatedDimensionSqlFn<
  TContext,
  TModelNames extends string,
  TDimensionNames extends string,
> = (
  args: CalculatedDimensionSqlFnArgs<TContext, TModelNames, TDimensionNames>,
) => SqlFn;

export type AnyCalculatedDimensionSqlFn = CalculatedDimensionSqlFn<
  any,
  string,
  string
>;

export type CalculatedDimensionProps<
  TContext,
  TModelNames extends string,
  TDimensionNames extends string,
> = MemberProps<{
  sql: CalculatedDimensionSqlFn<TContext, TModelNames, TDimensionNames>;
}>;

export class CalculatedDimension extends Dimension {
  constructor(
    readonly path: string,
    readonly props: CalculatedDimensionProps<any, any, any>,
  ) {
    super();
  }
  getAlias() {
    return pathToAlias(this.path);
  }
  getPath() {
    return this.path;
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
  isPrimaryKey() {
    return false;
  }
  isGranularity() {
    return false;
  }
  isDimension() {
    return true;
  }
  isMetric() {
    return false;
  }
  getQueryMember(
    queryMembers: QueryMemberCache,
    repository: AnyRepository,
    dialect: AnyBaseDialect,
    context: unknown,
  ): CalculatedDimensionQueryMember {
    return new CalculatedDimensionQueryMember(
      queryMembers,
      repository,
      dialect,
      context,
      this,
    );
  }
}

export class CalculatedDimensionQueryMember extends QueryMember {
  private sqlFnResult: SqlFn;
  private sqlFnRenderResult: SqlFragment;
  constructor(
    readonly queryMembers: QueryMemberCache,
    readonly repository: AnyRepository,
    readonly dialect: AnyBaseDialect,
    readonly context: unknown,
    readonly member: CalculatedDimension,
  ) {
    super();
    this.sqlFnResult = this.callSqlFn();

    this.sqlFnRenderResult = this.sqlFnResult.render(
      this.repository,
      this.queryMembers,
      this.dialect,
    );
  }
  private callSqlFn(): SqlFn {
    const models = this.repository.getModels();

    return this.member.props.sql({
      sql: (strings, ...values) => new SqlFn([...strings], values),
      identifier: (name) => new IdentifierRef(name),
      getContext: () => this.context,
      models: models.reduce<CalculateDimensionSqlFnArgsModels<string, string>>(
        (acc, model) => {
          acc[model.name] = {
            dimension: (name: string) =>
              new DimensionRef(model.getDimension(name), this.context),
            column: (name: string) => new ColumnRef(model, name, undefined),
          };
          return acc;
        },
        {},
      ),
    });
  }
  getAlias() {
    return this.member.getAlias();
  }
  getSql() {
    return this.sqlFnRenderResult;
  }
  getFilterSql() {
    return SqlFragment.fromSql("");
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
  getReferencedModels() {
    const filterFn = (ref: unknown): ref is DimensionRef | ColumnRef =>
      ref instanceof DimensionRef || ref instanceof ColumnRef;
    const refs = this.sqlFnResult.filterRefs(filterFn);
    const referencedModels = Array.from(
      new Set(
        refs.flatMap((ref) => {
          if (ref instanceof DimensionRef) {
            const dimensionQueryMember = this.queryMembers.get(ref.dimension);
            return dimensionQueryMember.getReferencedModels();
          }
          return [ref.model.name];
        }),
      ),
    );
    invariant(
      isNonEmptyArray(referencedModels),
      `Referenced models not found for ${this.member.getPath()}`,
    );
    return referencedModels;
  }
}
