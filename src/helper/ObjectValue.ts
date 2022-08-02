import {areEqual, fieldsHashCode, Vector} from "prelude-ts";
import {scalarComparison} from "./Collection";
import {WritableProps} from "./TypeMapping";

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

  I tried to do this by adding `this` as an arg to `copyWith`:
    `this: ObjectValue<Props> & {constructor: new (props: Props) => any}`
  This lets me remove the cast in that function (return new  ...) but causes a
  compilation error when `copyWith` is called on any type since TS thinks that
  the type of `this.constructor` is `Function`.
*/
export class ObjectValueBase<Props extends {}> {
  constructor(props: Props) {
    Object.assign(this, props);
  }

  copy(diffs: Partial<WritableProps<Props>>): ObjectValueBase<Props> & Props {
    const props: Props = {} as Props;
    Object.assign(props, this, diffs);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const this1 = new (this.constructor as any)(props);
    return this1;
  }

  private orderedFieldValues(): ReadonlyArray<unknown> {
    return (
      Object.getOwnPropertyNames(this)
        .sort(scalarComparison)
        // @ts-ignore Allow indexing by property name.
        .map((name) => this[name])
    );
  }

  // ---------------------------------------------------------------------------
  // For prelude-ts

  // These methods are surely very slow. We can optimize and perhaps cache them
  // in the future.

  equals(other: unknown): boolean {
    if (Object.getPrototypeOf(this) === Object.getPrototypeOf(other)) {
      const ourFields = Vector.ofIterable(this.orderedFieldValues());
      const otherFields = Vector.ofIterable(
        (other as this).orderedFieldValues(),
      );
      return ourFields.zip(otherFields).allMatch(([ours, other]) => {
        // areEqual is null-safe but not undefined-safe, so...
        if ((ours === undefined) !== (other === undefined)) return false;
        if (ours === other) return true;
        return areEqual(ours, other);
      });
    } else {
      return false;
    }
  }

  hashCode(): number {
    return fieldsHashCode(...this.orderedFieldValues());
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
