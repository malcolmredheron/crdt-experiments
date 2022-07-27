import {ReadonlyKeys, WritableKeys} from "ts-essentials";

export type WritableProps<T> = Pick<T, WritableKeys<T>>;
export type ReadonlyProps<T extends object> = Pick<T, ReadonlyKeys<T>>;

// Sadly, Typescript treats {} as an object that can contain any fields.
// Thus, we have to do extra work to prevent random fields when all fields
// are readonly. Thus, any type mapping that might return an empty type needs to
// be wrapped in this.
//
// https://stackoverflow.com/a/62404464
export type ReallyEmptyIfEmpty<T> = keyof T extends never
  ? Record<string, never>
  : T;
