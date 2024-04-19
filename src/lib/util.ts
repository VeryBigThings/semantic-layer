export function getAdHocPath(dimensionPath: string, aggregateWith: string) {
  return `${dimensionPath}.adhoc_${aggregateWith}`;
}
export function getAdHocAlias(dimensionPath: string, aggregateWith: string) {
  return `${dimensionPath.replaceAll(".", "___")}___adhoc_${aggregateWith}`;
}
