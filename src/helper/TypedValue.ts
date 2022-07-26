import {HasEquals} from "prelude-ts";

export class TypedValue<Name extends string, T> implements HasEquals {
  static create<Name extends string, T>(
    this: new (raw: T) => TypedValue<Name, T>,
    raw: T,
  ): TypedValue<Name, T> {
    return raw as unknown as TypedValue<Name, T>;
  }

  readonly typeName: Name = null as unknown as Name;
  readonly value: T = null as unknown as T;

  equals(other: TypedValue<Name, T>): boolean {
    throw "Calling equals on TypedValue";
  }

  hashCode(): number {
    throw "Calling hashCode on TypedValue";
  }
}

export function value<Name extends string, T>(
  typedValue: TypedValue<Name, T>,
): T {
  return typedValue as unknown as T;
}
