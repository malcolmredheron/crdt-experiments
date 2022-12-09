import {areEqual, fieldsHashCode, HashSet} from "prelude-ts";
import {scalarComparison} from "./Collection";
import {WritableProps} from "./TypeMapping";
import {MemoizeInstance} from "./Memoize";

/*
ObjectValue is for making values that are objects. Being a value means that it's
immutable and supports equals and hashCode.

To make it usable, it also supports making copies with some of the properties
updated. All properties that are declared in Props are readonly on instances of
the class, but properties that are declared as `readonly` in Props are not
allowed to by modifed by `copy`.

Design notes:
- We create an intermediate class (the subclass of ObjectValueBase) in
  ObjectValue because Typescript doesn't seem to let us define a class with
  properties that are defined by a generic param. For example,
  https://stackoverflow.com/questions/71571789/how-to-derive-properties-from-parameter-of-generic-class
  is about this and has no good answers.

  However, TS does let us return a class that will have these properties defined
  and then cast that class to one that TS believes will have the properties
  defined. So we do it this way.

TODOs
- Cause a compilation error if the subclass doesn't have a constructor that
  takes just the properties.

  I tried to do this by adding the commented-out `this` param on copy but, as
  described in https://github.com/microsoft/TypeScript/issues/3841, this doesn't
  work.
*/
class ObjectValueBase<Props extends {}> {
  constructor(props: Props) {
    Object.assign(this, props);
  }

  // Add fields to this if a subclass has fields that can't be compared using
  // standard equality. (And override .equals() in that case!)
  excludeFromEquals = HashSet.of<string>();

  copy(
    // this: {constructor: {new (props: Props): ObjectValueBase<Props>}},
    diffs: Partial<WritableProps<Props>>,
  ): this {
    const props: Props = {} as Props;
    Object.assign(props, this, diffs);
    return new (this.constructor as {
      new (props: Props): ObjectValueBase<Props>;
    })(props) as this;
  }

  @MemoizeInstance
  toString(): string {
    // We want different intances to have different strings so that they stay
    // separate when used as keys, such as in expectIdenticalMismatches. We
    // could do this by fully serializing the object, but that produces a lot of
    // text. Instead we just use a random number. We memoize it so that it's at
    // least stable.
    return `${this.constructor.name} ${Math.random()}`;
  }

  private orderedFieldValues(): ReadonlyArray<[string, unknown]> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const constructor = (this as any).__proto__.constructor;
    let sortedPropertyNames: ReadonlyArray<string> =
      constructor._sortedPropertyNames;
    if (sortedPropertyNames === undefined) {
      sortedPropertyNames =
        Object.getOwnPropertyNames(this).sort(scalarComparison);
      constructor._sortedPropertyNames = sortedPropertyNames;
    }

    // @ts-ignore Allow indexing by property name.
    return sortedPropertyNames.map((name) => [name, this[name]]);
  }

  // ---------------------------------------------------------------------------
  // For prelude-ts

  // These methods are surely very slow. We can optimize and perhaps cache them
  // in the future.

  equals(other: unknown): boolean {
    if (Object.getPrototypeOf(this) === Object.getPrototypeOf(other)) {
      const ourFields = this.orderedFieldValues();
      const otherFields = (other as this).orderedFieldValues();
      for (let i = 0; i < ourFields.length; i++) {
        if (this.excludeFromEquals.contains(ourFields[i][0])) continue;
        const ours = ourFields[i][1];
        const other = otherFields[i][1];
        if ((ours === undefined) !== (other === undefined)) return false;
        if (ours === other) continue;
        if (!areEqual(ours, other)) return false;
      }
      return true;
    } else {
      return false;
    }
  }

  hashCode(): number {
    return fieldsHashCode(
      ...this.orderedFieldValues().map((tuple) => tuple[1]),
    );
  }
}

export function ObjectValue<Props extends {}>(): {
  new (props: Props): ObjectValueBase<Props> & Props;
  prototype: ObjectValueBase<Props>;
} {
  return ObjectValueBase as unknown as {
    new (props: Props): ObjectValueBase<Props> & Props;
    prototype: ObjectValueBase<Props>;
  };
}
