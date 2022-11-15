import {AssertFailed} from "./Assert";
import {HashMap} from "prelude-ts";

export function asType<T>(value: T): T {
  return value;
}

// Using this instead of the raw spread operator protects against accidentally
// adding fields that are not part of T.
export function updated<T extends object>(obj: T, patch: Partial<T>): T {
  return {...obj, ...patch};
}

export function definedOrThrow<V>(
  valueOrUndefined: V | undefined,
  errorMessage: string,
): V {
  if (valueOrUndefined === undefined) {
    throw new AssertFailed(errorMessage);
  }
  return valueOrUndefined;
}

export function throwError<T>(message: string): T {
  throw new AssertFailed(message);
}

type Scalar = boolean | number | string;

// Booleans, strings, and numbers are all comparable, but not with each other.
// Follows the standard cmp contract for return value
export function scalarComparison<T extends Scalar>(a: T, b: T): number {
  return a < b ? -1 : b < a ? 1 : 0;
}

// Transforms a value through a sequence of functions.
//
// Similar to reduce, but more readable when using an array of functions.
export function transformedValue<T>(
  value: T,
  ...functions: ReadonlyArray<(node: T) => T>
): T {
  return functions.reduce((value, f) => f(value), value);
}

// This looks stupid but it's a bit more readable than defining a function and
// calling it in the same expression.
export function transformedOnce<T, U>(value: T, func: (value: T) => U): U {
  return func(value);
}

// Maps the values in the map, returning a new value only if func returned a
// different value for at least one of the entries.
export function mapValuesStable<K, V>(
  map: HashMap<K, V>,
  func: (v: V) => V,
): HashMap<K, V> {
  let changed = false;
  const map1 = map.mapValues((v) => {
    const v1 = func(v);
    if (v1 !== v) changed = true;
    return v1;
  });
  return changed ? map1 : map;
}
