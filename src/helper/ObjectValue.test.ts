import {ObjectValue} from "./ObjectValue";
import {expectIdentical, expectPreludeEqual} from "./Shared.testing";
import {Vector} from "prelude-ts";

describe("ObjectValue", () => {
  let initedNames = Vector<string>.of();

  type NameProps = {first: string; last: string};
  class Name extends ObjectValue<NameProps>() {
    constructor(props: NameProps) {
      super(props);
      initedNames = initedNames.append(`${this.first} ${this.last}`);
    }

    // This is required to prevent a mistyped return value from copy() from
    // structurally matching with Name.
    method(greeting: string): string {
      return `${greeting}, ${this.first}`;
    }
  }

  it("creates new instance", () => {
    initedNames = Vector.of();
    const name = new Name({first: "agent", last: "smith"});
    expectIdentical(name.first, "agent");
    expectIdentical(name.last, "smith");
    // Check that the constructor got called and that the property values were
    // in place by that point.
    expectPreludeEqual(initedNames, Vector.of("agent smith"));
    expectIdentical(name instanceof Name, true);
  });

  it("can copy with changes", () => {
    initedNames = Vector.of();
    const name = new Name({first: "agent", last: "smith"});
    const name1 = name.copy({last: "orange"});
    expectIdentical(name1.first, "agent");
    expectIdentical(name1.last, "orange");
    // Check that the constructor got called and that the property values were
    // in place by that point.
    expectPreludeEqual(initedNames, Vector.of("agent smith", "agent orange"));
  });

  it("copy() returns the same type as the original", () => {
    const name: Name = new Name({first: "agent", last: "smith"});
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const name1: Name = name.copy({first: "mr"});
  });

  it("allows writable fields in create, not in copyWith", () => {
    class Vc extends ObjectValue<{
      mutable: string;
      readonly immutable: string;
    }>() {}
    const vc = new Vc({mutable: "a", immutable: "b"});
    vc.copy({mutable: "b1"});
    // @ts-expect-error: immutable fields can't be changed with `copyWith`.
    vc.copy({immutable: "a1"});
  });

  it("equals and hashCode work", () => {
    class Vc extends ObjectValue<{
      mutable: string;
      readonly immutable: string;
    }>() {}

    const vc = new Vc({mutable: "hello", immutable: "there"});
    const vcSame = new Vc({mutable: "hello", immutable: "there"});
    const vcModified = vc.copy({mutable: "bye"});

    expectIdentical(vc.equals(vcSame), true);
    expectIdentical(vcSame.equals(vc), true);
    expectIdentical(vc.hashCode(), vcSame.hashCode());

    expectIdentical(!vc.equals(vcModified), true);
    expectIdentical(vcModified.equals(vc), false);
    expectIdentical(vc.hashCode() === vcModified.hashCode(), false);

    expectIdentical(vc.equals({}), false);
  });
});
