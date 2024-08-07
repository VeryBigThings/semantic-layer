import { MemberNameToType, MemberType, MemberTypeToType } from "./types.js";

import { AnyModel } from "./model.js";
import { AnyRepository } from "./repository.js";

export interface CustomGranularityElementConfig {
  name: string;
  dimensions: string[];
  keyDimensions: string[];
  formatDimensions: string[];
  formatter: (row: Record<string, unknown>) => string;
}

export type AnyCustomGranularityElement = CustomGranularityElement<any, any>;

export class CustomGranularityElement<
  D extends MemberNameToType,
  DN extends keyof D,
> {
  private keys: string[] | null = null;
  private formatDimensions: string[];
  private formatter:
    | ((props: { dimension: (name: string) => any }) => string)
    | null = null;
  constructor(
    public readonly name: string,
    private readonly dimensionNames: string[],
  ) {
    this.formatDimensions = dimensionNames;
  }
  withKey<K extends DN>(keys: (K & string)[]) {
    this.keys = keys;
    return this;
  }
  withFormat<FD extends DN>(
    dimensions: (FD & string)[],
    formatter?: (props: {
      dimension: <FD1 extends FD, DT1 = D[FD1 & string]>(
        name: FD1 & string,
      ) => {
        originalValue: MemberTypeToType<DT1 & MemberType> | null;
        formattedValue: string | null;
      };
    }) => string,
  ) {
    this.formatDimensions = dimensions;
    this.formatter = formatter ?? null;
    return this;
  }
  getDefaultFormatter(parent: AnyModel | AnyRepository) {
    return (row: Record<string, unknown>) =>
      this.formatDimensions
        .map((dimensionName) => {
          const dimension = parent.getDimension(dimensionName);
          const originalValue = row[dimension.getAlias()] ?? null;
          const formattedValue =
            originalValue === null || originalValue === undefined
              ? null
              : dimension.props.format
                ? dimension.unsafeFormatValue(originalValue)
                : null;
          return formattedValue ?? originalValue;
        })
        .join(", ");
  }
  getFormatter(parent: AnyModel | AnyRepository) {
    const formatter = this.formatter;
    if (formatter) {
      return (row: Record<string, unknown>) => {
        return formatter({
          dimension: (dimensionName: string) => {
            const dimension = parent.getDimension(dimensionName);
            const originalValue = row[dimension.getAlias()] ?? null;
            const formattedValue =
              originalValue === null || originalValue === undefined
                ? null
                : dimension.props.format
                  ? dimension.unsafeFormatValue(originalValue)
                  : null;
            return {
              originalValue,
              formattedValue,
            };
          },
        });
      };
    }
    return this.getDefaultFormatter(parent);
  }
  getConfig(parent: AnyModel | AnyRepository): CustomGranularityElementConfig {
    const dimensionNames = this.dimensionNames.map((dimensionName) =>
      parent.getDimension(dimensionName).getPath(),
    );
    return {
      name: this.name,
      dimensions: dimensionNames,
      keyDimensions:
        this.keys?.map((dimensionNames) =>
          parent.getDimension(dimensionNames).getPath(),
        ) ?? dimensionNames,
      formatDimensions: this.formatDimensions.map((dimensionName) =>
        parent.getDimension(dimensionName).getPath(),
      ),
      formatter: this.getFormatter(parent),
    };
  }
}

export class CustomGranularityElementInit<D extends MemberNameToType> {
  constructor(public readonly name: string) {}

  withDimensions<DN extends keyof D>(dimensionNames: (DN & string)[]) {
    return new CustomGranularityElement<D, DN>(this.name, dimensionNames);
  }
}

export function makeCustomGranularityElementInitMaker<
  D extends MemberNameToType,
>() {
  const fn = (name: string) => new CustomGranularityElementInit<D>(name);

  fn.fromDimension = <DN extends keyof D>(name: DN & string) =>
    new CustomGranularityElementInit<D>(name as string).withDimensions([name]);

  return fn;
}
