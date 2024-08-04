import { IntrospectionResult } from "./types.js";

function formatPercentage(value: unknown) {
  return `${value}%`;
}

function formatCurrency(value: unknown) {
  return `$${value}`;
}

const defaultFormats = {
  percentage: formatPercentage,
  currency: formatCurrency,
} as const;

export function formatResults<T extends Record<string, unknown>>(
  results: T[],
  introspection: IntrospectionResult,
) {
  const introspectionEntries = Object.entries(introspection);
  const formatted = results.map((result) => {
    return Object.fromEntries(
      introspectionEntries.map(([key, introspectionValue]) => {
        const formatter = introspectionValue.format;
        const value = result[key];
        if (formatter) {
          const formatterFn =
            typeof formatter === "function"
              ? formatter
              : defaultFormats[formatter];
          return [
            key,
            {
              value,
              formattedValue: formatterFn(value as any),
            },
          ];
        }
        return [
          key,
          {
            value,
          },
        ];
      }),
    );
  });
  return formatted as {
    [K in keyof T]: { value: T[K]; formattedValue?: string };
  }[];
}

export function pathToAlias(path: string) {
  return path.replaceAll(".", "___");
}
