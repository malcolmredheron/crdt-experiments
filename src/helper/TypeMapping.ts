export type WritableProps<T> = Pick<T, WritableKeys<T>>;
export type ReadonlyProps<T> = Pick<T, ReadonlyKeys<T>>;

// Sadly, Typescript treats {} as an object that can contain any fields.
// Thus, we have to do extra work to prevent random fields when all fields
// are readonly. Thus, any type mapping that might return an empty type needs to
// be wrapped in this.
//
// https://stackoverflow.com/a/62404464
export type ReallyEmptyIfEmpty<T> = keyof T extends never
  ? Record<string, never>
  : T;

// From:
// - https://stackoverflow.com/a/49579497
// - https://stackoverflow.com/a/52473108
type IfEquals<X, Y, A = X, B = never> = (<T>() => T extends X ? 1 : 2) extends <
  T,
>() => T extends Y ? 1 : 2
  ? A
  : B;

export type WritableKeys<T> = {
  [P in keyof T]-?: IfEquals<{[Q in P]: T[P]}, {-readonly [Q in P]: T[P]}, P>;
}[keyof T];

export type ReadonlyKeys<T> = {
  [P in keyof T]-?: IfEquals<
    {[Q in P]: T[P]},
    {-readonly [Q in P]: T[P]},
    never,
    P
  >;
}[keyof T];

export function readonlyObject<T>(object: T): Readonly<T> {
  return object;
}

export type MutableProps<Props extends object> = Readonly<{
  [P in WritableKeys<Props>]: Props[P];
}>;
