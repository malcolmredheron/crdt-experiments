/*
Tries to mimic Scala's case classes. In particular, makes it easy to define a
class that consists of a several properties. All properties will be readonly on
the resulting object, but ones that are declared as readonly will only be able
to be specified when creating an object. Ones that are not declared as readonly
can be changed with copyWith (changed in the copy, that is).

TODOs
- Cause a compilation error if the subclass doesn't have a constructor that
  takes just the properties.

  I tried to do this by adding `this` as an arg to `copyWith`:
    `this: CaseClass<Props> & {constructor: new (props: Props) => any}`
  This lets me remove the cast in that function (return new  ...) but causes a
  compilation error when `copyWith` is called on any type since TS thinks that
  the type of `this.constructor` is `Function`.
- Remove .p. Ideally, we'd use Object.assign to copy values directly into the
  object. Here is what we have tried so far:
  - TS doesn't seem to let us declare a class that has instance variables that
    are defined by a generic. https://stackoverflow.com/questions/71571789/how-to-derive-properties-from-parameter-of-generic-class
    is trying to do the same thing and got no good answers.
  - Declaring the properties in the class instead of in the generic params. When
    we do this we have to ts-ignore each line in a separate comment above the
    line since there is no initializer in the constructor. We can't init the
    props with dummy values because these will run after the inherited
    constructor and overwrite the work done there.
*/

import {asType, scalarComparison} from "./Collection";
import {ReadonlyProps, ReallyEmptyIfEmpty, WritableProps} from "./TypeMapping";
import {areEqual, fieldsHashCode, Vector} from "prelude-ts";

export class CaseClass<Props extends object> {
  constructor(props: Readonly<Props>) {
    this.p = props;
  }

  public copy(
    modifyObject:
      | ReallyEmptyIfEmpty<Partial<WritableProps<Props>>>
      | ((object: this) => ReallyEmptyIfEmpty<Partial<WritableProps<Props>>>),
  ): this {
    const props = asType<ReadonlyProps<Props>>({
      ...this.p,
      ...(typeof modifyObject === "function"
        ? modifyObject(this)
        : modifyObject),
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return new (this.constructor as any)(props);
  }

  private orderedFieldValues(): ReadonlyArray<unknown> {
    return (
      Object.getOwnPropertyNames(this.p)
        .sort(scalarComparison)
        // @ts-ignore Allow indexing by property name.
        .map((name) => this.p[name])
    );
  }

  readonly p: Readonly<Props>;

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
