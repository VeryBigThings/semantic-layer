import { AnyBaseDialect } from "../../dialect/base.js";
import { Member } from "../../member.js";
import { AnyRepository } from "../../repository.js";
import { SqlFragment } from "../../sql-builder.js";

export class QueryMemberCache {
  private cache = new Map<Member, QueryMember>();
  constructor(
    private readonly repository: AnyRepository,
    private readonly dialect: AnyBaseDialect,
    private readonly context: unknown,
  ) {}
  getByPath(memberPath: string) {
    const member = this.repository.getMember(memberPath);
    return this.get(member);
  }
  get(member: Member) {
    const cached = this.cache.get(member);
    if (cached) {
      return cached;
    }
    const queryMember = member.getQueryMember(
      this,
      this.repository,
      this.dialect,
      this.context,
    );
    this.cache.set(member, queryMember);
    return queryMember;
  }
}

export abstract class QueryMember {
  abstract readonly queryMembers: QueryMemberCache;
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
}
