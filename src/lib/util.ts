export function exhaustiveCheck(
  _exhaustiveCheck: never,
  message: string,
): never {
  throw new Error(message);
}
