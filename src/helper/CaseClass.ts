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

Notes:
- We use `create` instead `new` because `new` will infer template properties
  when constructing an instance of a class that leaves its props type open (such
  as `Parent` in the tests for this file). `create`, OTOH, will not.
 */

import {asType} from "./Collection";
import {MutableProps, ReadonlyProps, ReallyEmptyIfEmpty} from "./TypeMapping";

export class CaseClass<Props extends object> {
  static create<Props extends object, CC>(
    this: new (props: Readonly<Props>) => CC,
    props: Readonly<Props>,
  ): CC {
    return new this(props);
  }

  constructor(props: Readonly<Props>) {
    this.p = props;
  }

  public copyWith(
    modifyObject:
      | ReallyEmptyIfEmpty<Partial<MutableProps<Props>>>
      | ((object: this) => ReallyEmptyIfEmpty<Partial<MutableProps<Props>>>),
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

  readonly p: Readonly<Props>;
}
