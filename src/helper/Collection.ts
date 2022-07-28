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

export function RoArray<T>(values?: Iterable<T>): RoArray<T> {
  return values === undefined ? [] : Array.from(values);
}

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

export type ReadonlyType<T> = T extends Array<infer V>
  ? ReadonlyArray<V>
  : T extends Map<infer K, infer V>
  ? ReadonlyMap<K, V>
  : T extends Set<infer V>
  ? ReadonlySet<V>
  : T;

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

export function readonly<V>(collection: Array<V>): ReadonlyArray<V>;
export function readonly<K, V>(collection: Map<K, V>): ReadonlyMap<K, V>;
export function readonly<V>(collection: Set<V>): ReadonlySet<V>;
export function readonly(
  collection: Array<unknown> | Map<unknown, unknown> | Set<unknown>,
):
  | ReadonlyArray<unknown>
  | ReadonlyMap<unknown, unknown>
  | ReadonlySet<unknown> {
  return collection;
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

export function arrayFirst<V>(array: RoArray<V>): V | undefined {
  return array.length === 0 ? undefined : array[0];
}

export function arrayLast<V>(array: RoArray<V>): V | undefined {
  return array[array.length - 1];
}

export function arrayOnly<V>(array: RoArray<V>): V | undefined {
  if (array.length === 1) {
    return array[0];
  } else {
    return undefined;
  }
}

export function arrayLastOrThrow<V>(array: RoArray<V>): V {
  if (array.length === 0)
    throw new AssertFailed("Cannot get last from empty array");
  return array[array.length - 1];
}

export function arrayPushAll<V>(array: Array<V>, values: Iterable<V>): void {
  for (const v of values) {
    array.push(v);
  }
}

export function arrayRemove<V>(array: Array<V>, index: number): void {
  array.splice(index, 1);
}

export function arraySorted<V>(
  array: RoArray<V>,
  comparator: (a: V, b: V) => number,
): RoArray<V> {
  const mutable = writable(array);
  mutable.sort(comparator);
  return mutable;
}

export function mapMapToMap<K, V, K1, V1>(
  map: RoMap<K, V>,
  func: (key: K, value: V) => [K1, V1],
): RoMap<K1, V1> {
  return new Map(
    Array.from(map.entries()).map((entry) => func(entry[0], entry[1])),
  );
}

export function mapWith<K, V>(map: RoMap<K, V>, key: K, value: V): RoMap<K, V> {
  return new Map(map).set(key, value);
}

export function mapWithout<K, V>(map: RoMap<K, V>, key: K): RoMap<K, V> {
  return RoMap(Array.from(map).filter(([k, v]) => k !== key));
}

export function setAddAll<V>(set: Set<V>, values: Iterable<V>): void {
  for (const v of values) {
    set.add(v);
  }
}

// Returns left - right.
export function setDifference<V>(left: RoSet<V>, right: Iterable<V>): RoSet<V> {
  const output = writable(left);
  for (const v of right) {
    output.delete(v);
  }
  return output;
}

export function setFirst<V>(set: RoSet<V>): V | undefined {
  if (set.size > 0) {
    return set.values().next().value;
  } else {
    return undefined;
  }
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
