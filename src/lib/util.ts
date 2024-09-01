export function exhaustiveCheck(
  _exhaustiveCheck: never,
  message: string,
): never {
  throw new Error(message);
}

export const METRIC_REF_SUBQUERY_ALIAS = "__mrs__";

export function isNonEmptyArray<T>(value: T[]): value is [T, ...T[]] {
  return value.length > 0;
}
