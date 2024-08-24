export function exhaustiveCheck(
  _exhaustiveCheck: never,
  message: string,
): never {
  throw new Error(message);
}

export const METRIC_REF_SUBQUERY_ALIAS = "__mrs__";
