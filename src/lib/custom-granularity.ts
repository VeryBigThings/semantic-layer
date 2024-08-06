import { AnyModel } from "./model.js";
import { AnyRepository } from "./repository.js";
import invariant from "tiny-invariant";

export interface CustomGranularityElementConfig {
  name: string;
  dimensions: string[];
  keyDimensions: string[];
  formatDimensions: string[];
  formatter: (row: Record<string, unknown>) => string;
}

export type AnyCustomGranularityElement = CustomGranularityElement<any>;

export abstract class CustomGranularityElementFormatter {
  abstract getFormatter(
    parent: AnyModel | AnyRepository,
  ): (row: Record<string, unknown>) => string;
  abstract getReferencedDimensionNames(): string[];
}

export class CustomGranularityElementDimensionRef extends CustomGranularityElementFormatter {
  constructor(public readonly dimensionName: string) {
    super();
  }
  getFormatter(parent: AnyModel | AnyRepository) {
    const dimension = parent.getDimension(this.dimensionName);
    return (row: Record<string, unknown>) => {
      const value = row[dimension.getAlias()];
      return dimension.unsafeFormatValue(value);
    };
  }
  getReferencedDimensionNames() {
    return [this.dimensionName];
  }
}
export class CustomGranularityElementTemplateWithDimensionRefs extends CustomGranularityElementFormatter {
  constructor(
    public readonly strings: string[],
    public readonly values: unknown[],
  ) {
    super();
  }
  getReferencedDimensionNames() {
    const dimensions: string[] = [];
    for (const value of this.values) {
      if (value instanceof CustomGranularityElementDimensionRef) {
        dimensions.push(value.dimensionName);
      }
    }
    return dimensions;
  }
  getFormatter(parent: AnyModel | AnyRepository) {
    return (row: Record<string, unknown>) => {
      const result = [];
      for (let i = 0; i < this.strings.length; i++) {
        result.push(this.strings[i]!);
        const nextValue = this.values[i];
        if (nextValue) {
          if (nextValue instanceof CustomGranularityElementDimensionRef) {
            const dimension = parent.getDimension(nextValue.dimensionName);
            const value = row[dimension.getAlias()];
            result.push(dimension.unsafeFormatValue(value));
          } else {
            result.push(nextValue);
          }
        }
      }
      return result.join("");
    };
  }
}

export class CustomGranularityElement<D extends string> {
  private readonly dimensionRefs: Record<
    string,
    CustomGranularityElementDimensionRef
  >;
  private keys: string[] | null = null;
  private formatter: CustomGranularityElementFormatter | null = null;
  constructor(
    public readonly name: string,
    private readonly dimensionNames: string[],
  ) {
    this.dimensionRefs = dimensionNames.reduce<
      Record<string, CustomGranularityElementDimensionRef>
    >((acc, dimensionName) => {
      acc[dimensionName] = new CustomGranularityElementDimensionRef(
        dimensionName,
      );
      return acc;
    }, {});
  }
  withKey<K extends D>(...keys: K[]) {
    this.keys = keys;
    return this;
  }
  withFormat(
    formatter: (props: {
      dimension: (name: D) => CustomGranularityElementDimensionRef;
      template: (
        strings: TemplateStringsArray,
        ...values: unknown[]
      ) => CustomGranularityElementTemplateWithDimensionRefs;
    }) =>
      | CustomGranularityElementDimensionRef
      | CustomGranularityElementTemplateWithDimensionRefs,
  ) {
    this.formatter = formatter({
      dimension: (name: D) => {
        const dimensionRef = this.dimensionRefs[name];
        invariant(dimensionRef, `Dimension ${name} not found`);
        return dimensionRef;
      },
      template: (strings, ...values) => {
        return new CustomGranularityElementTemplateWithDimensionRefs(
          [...strings],
          values,
        );
      },
    });
    return this;
  }
  gerDefaultFormatter(parent: AnyModel | AnyRepository) {
    return (row: Record<string, unknown>) =>
      this.dimensionNames
        .map((dimensionName) =>
          parent
            .getDimension(dimensionName)
            .unsafeFormatValue(row[dimensionName]),
        )
        .join(", ");
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
      formatDimensions:
        this.formatter?.getReferencedDimensionNames() ?? dimensionNames,
      formatter:
        this.formatter?.getFormatter(parent) ??
        this.gerDefaultFormatter(parent),
    };
  }
}

export class CustomGranularityElementInit<D extends string> {
  constructor(
    public readonly parent: AnyModel | AnyRepository,
    public readonly name: string,
  ) {}
  withDimensions<GD extends D>(...dimensionNames: GD[]) {
    return new CustomGranularityElement<GD>(this.name, dimensionNames);
  }
}
