import { AnyBaseDialect } from "../../dialect/base.js";
import { Member } from "../../member.js";
import { AnyRepository } from "../../repository.js";
import { QueryMember } from "./query-member.js";

export class QueryContext {
  private queryMembersCache = new Map<Member, QueryMember>();
  constructor(
    private readonly repository: AnyRepository,
    private readonly dialect: AnyBaseDialect,
    private readonly context: unknown,
  ) {}

  getQueryMemberByPath(memberPath: string) {
    const member = this.repository.getMember(memberPath);
    return this.getQueryMember(member);
  }
  getQueryMember(member: Member) {
    const cached = this.queryMembersCache.get(member);
    if (cached) {
      return cached;
    }
    const queryMember = member.getQueryMember(
      this,
      this.repository,
      this.dialect,
      this.context,
    );
    this.queryMembersCache.set(member, queryMember);
    return queryMember;
  }
}
