import {expect} from "chai";
import deepEqual from "deep-equal";
import {RoMap, RoSet, setDifference, setOnly} from "./Collection";

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
  } else if (actual instanceof Set && expected instanceof Set) {
    const onlyActual = setDifference(actual, expected);
    const onlyExpected = setDifference(expected, actual);
    const singleOnlyActual = setOnly(onlyActual);
    const singleOnlyExpected = setOnly(onlyExpected);
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
    const actualMap = RoMap(Object.entries(actual));
    const expectedMap = RoMap(Object.entries(expected));
    const keys = RoSet([...actualMap.keys(), ...expectedMap.keys()]);
    for (const key of keys) {
      const actualValue = actualMap.get(key);
      const expectedValue = expectedMap.get(key);
      mismatches[key] = expectIdenticalMismatches(actualValue, expectedValue);
    }
  }

  return mismatches;
}
