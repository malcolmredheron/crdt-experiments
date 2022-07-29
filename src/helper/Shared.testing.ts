import {expect} from "chai";
import deepEqual from "deep-equal";
import {HasEquals, HashMap, HashSet} from "prelude-ts";

// We have some functions that are useful to call in the console from tests
// while debugging. However, the compiler complains if the symbols aren't used.
// This gets around that.
//
// eslint-disable-next-line @typescript-eslint/no-empty-function
export function touch(...v: unknown[]): void {}

// Like `expect(...).equal(...)` but statically typed.
export function expectIdentical<T>(actual: T, expected: T): void {
  const mismatches = expectIdenticalMismatches(actual, expected);
  if (mismatches !== null) {
    debugger;
    expect(mismatches).equal(null);
  }
}

// Like `expect(...).deep.equal(...)` but statically typed.
export function expectDeepEqual<T>(actual: T, expected: T): void {
  if (!deepEqual(actual, expected, {strict: true})) {
    debugger;
    expect(actual).deep.equal(expected);
  }
}

export function expectPreludeEqual<T extends HasEquals>(
  actual: T,
  expected: T,
): void {
  if (!actual.equals(expected)) {
    debugger;
    expect(actual).deep.equal(expected);
  }
}

export function expectIdenticalMismatches<T>(
  actual: T,
  expected: T,
): null | object {
  if (actual === expected) return null;
  if (typeof actual !== typeof expected) return {actual, expected};

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const mismatches: any = {};

  if (typeof actual === "string" && typeof expected === "string") {
    return {actual, expected};
  } else if (typeof actual === "number" && typeof expected === "number") {
    return {actual, expected};
  } else if (actual instanceof HashSet && expected instanceof HashSet) {
    const onlyActual = actual.removeAll(expected);
    const onlyExpected = expected.removeAll(actual);
    const singleOnlyActual = onlyActual.single().getOrUndefined();
    const singleOnlyExpected = onlyExpected.single().getOrUndefined();
    if (singleOnlyActual !== undefined && singleOnlyExpected !== undefined) {
      mismatches["only difference, difference"] = expectIdenticalMismatches(
        singleOnlyActual,
        singleOnlyExpected,
      );
      mismatches["only difference, z actual"] = singleOnlyActual;
      mismatches["only difference, z expected"] = singleOnlyExpected;
    } else {
      let i = 0;
      for (const v of onlyActual) {
        mismatches["actual" + i++] = v;
      }
      for (const v of onlyExpected) {
        mismatches["expected" + i++] = v;
      }
    }
  } else {
    const actualMap = HashMap.ofIterable(Object.entries(actual));
    const expectedMap = HashMap.ofIterable(Object.entries(expected));
    const keys = actualMap.keySet().addAll(expectedMap.keySet());
    for (const key of keys) {
      const actualValue = actualMap.get(key).getOrUndefined();
      const expectedValue = expectedMap.get(key).getOrUndefined();
      mismatches[key] = expectIdenticalMismatches(actualValue, expectedValue);
    }
  }

  return mismatches;
}
