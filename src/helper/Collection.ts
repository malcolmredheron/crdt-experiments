import {AssertFailed} from "./Assert";

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

export type RoArray<T> = ReadonlyArray<T>;
export type RoMap<K, V> = ReadonlyMap<K, V>;
export type RoSet<T> = ReadonlySet<T>;

export function RoMap<K, V>(values?: Iterable<[K, V]>): RoMap<K, V> {
  if (values === undefined) return new Map();

  const valuesArray = Array.from(values);
  const keys = RoSet(valuesArray.map(([key, value]) => key));
  if (keys.size !== valuesArray.length)
    throw new AssertFailed("RoMap created with duplicate keys");

  return new Map(values);
}

export function RoSet<T>(values?: Iterable<T>): RoSet<T> {
  return new Set(values);
}

export function writable<V>(collection: ReadonlyArray<V>): Array<V>;
export function writable<K, V>(collection: ReadonlyMap<K, V>): Map<K, V>;
export function writable<V>(collection: ReadonlySet<V>): Set<V>;
export function writable(
  collection:
    | ReadonlyArray<unknown>
    | ReadonlyMap<unknown, unknown>
    | ReadonlySet<unknown>,
): Array<unknown> | Map<unknown, unknown> | Set<unknown> {
  if (collection instanceof Array) {
    return [...collection];
  } else if (collection instanceof Map) {
    return new Map(collection);
  } else if (collection instanceof Set) {
    return new Set(collection);
  }
  throw new AssertFailed("Unexpected input type");
}

type Scalar = boolean | number | string;

// Booleans, strings, and numbers are all comparable, but not with each other.
// Follows the standard cmp contract for return value
export function scalarComparison<T extends Scalar>(a: T, b: T): number {
  return a < b ? -1 : b < a ? 1 : 0;
}

// Follows the standard cmp contract for return value
export function arrayComparison<V>(
  // Expected to follow the standard cmp contract for return value
  itemComparison: (left: V, right: V) => number,
): (left: RoArray<V>, right: RoArray<V>) => number {
  return (left, right) => {
    for (let i = 0; i < Math.max(left.length, right.length); i++) {
      if (i >= left.length) return -1;
      if (i >= right.length) return 1;
      const comparison = itemComparison(left[i], right[i]);
      if (comparison !== 0) return comparison;
    }
    return 0;
  };
}

export function arrayEqual<V>(left: RoArray<V>, right: RoArray<V>): boolean {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index++) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}

// Returns left - right.
export function setDifference<V>(left: RoSet<V>, right: Iterable<V>): RoSet<V> {
  const output = writable(left);
  for (const v of right) {
    output.delete(v);
  }
  return output;
}

export function setOnly<V>(set: RoSet<V>): V | undefined {
  if (set.size === 1) {
    return set.values().next().value;
  } else {
    return undefined;
  }
}

// Transforms a value through a sequence of functions.
//
// Similar to reduce, but more readable when using an array of functions.
export function transformedValue<T>(
  value: T,
  ...functions: RoArray<(node: T) => T>
): T {
  return functions.reduce((value, f) => f(value), value);
}

// This looks stupid but it's a bit more readable than defining a function and
// calling it in the same expression.
export function transformedOnce<T, U>(value: T, func: (value: T) => U): U {
  return func(value);
}
