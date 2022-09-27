import {AssertFailed} from "./Assert";

// A simple memoization library.
//
// This is distinguished from the ones on the internet by its use of WeakMap
// isntead of modifying the objects. This is important because modifying the
// objects messes up unit tests, since the state of an object depends on which
// methods have been called on it.

// Memoizes the return value of an instance method that takes no arguments.
export function MemoizeInstance<O extends object, R>(
  target: object,
  propertyKey: string,
  descriptor: TypedPropertyDescriptor<(this: O) => R>,
): void {
  if (descriptor.value) {
    const cache = new WeakMap<O, R>();
    const originalValue = descriptor.value;
    descriptor.value = function (this: O): R {
      let result = cache.get(this);
      if (result === undefined) {
        result = originalValue.apply(this);
        cache.set(this, result);
      }
      return result;
    };
  } else {
    throw new AssertFailed("Does not apply");
  }
}

// Memoizes the return value of a static method that takes exactly one
// object-valued argument.
export function MemoizeStatic<O extends object, R>(
  target: object,
  propertyKey: string,
  descriptor: TypedPropertyDescriptor<(arg: O) => R>,
): void {
  if (descriptor.value) {
    const cache = new WeakMap<O, R>();
    const originalValue = descriptor.value;
    descriptor.value = function (arg: O): R {
      let result = cache.get(arg);
      if (result === undefined) {
        result = originalValue.apply(target, [arg]);
        cache.set(arg, result);
      }
      return result;
    };
  } else {
    throw new AssertFailed("Does not apply");
  }
}
