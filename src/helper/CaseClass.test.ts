import {CaseClass} from "./CaseClass";
import {expectIdentical} from "./Shared.testing";

describe("CaseClass", () => {
  it("allows writable fields in create, not in copyWith", () => {
    class CC extends CaseClass<{
      mutable: string;
      readonly immutable: string;
    }> {}
    const cc = new CC({mutable: "a", immutable: "b"});
    cc.copy({mutable: "b1"});
    // @ts-expect-error: immutable fields can't be changed with `copyWith`.
    cc.copy({immutable: "a1"});
  });

  it("prevents unknown fields in copyWith when all fields readonly", () => {
    // Sadly, Typescript treats {} as an object that can contain any fields.
    // Thus, we have to do extra work to prevent random fields when all fields
    // are readonly.

    class CC extends CaseClass<{
      readonly immutable: string;
    }> {}
    const cc = new CC({immutable: "b"});
    // @ts-expect-error: immutable fields can't be changed with `copyWith`.
    cc.copy({immutable: "a1"});
    // @ts-expect-error: unknown fields can't be changed with `copyWith`.
    cc.copy({junk: "a1"});
  });

  it("equals and hashCode work", () => {
    class CC extends CaseClass<{
      mutable: string;
      readonly immutable: string;
    }> {}

    const cc = new CC({mutable: "hello", immutable: "there"});
    const ccSame = new CC({mutable: "hello", immutable: "there"});
    const ccModified = cc.copy({mutable: "bye"});

    expectIdentical(cc.equals(ccSame), true);
    expectIdentical(ccSame.equals(cc), true);
    expectIdentical(cc.hashCode(), ccSame.hashCode());

    expectIdentical(!cc.equals(ccModified), true);
    expectIdentical(ccModified.equals(cc), false);
    expectIdentical(cc.hashCode() === ccModified.hashCode(), false);

    expectIdentical(cc.equals({}), false);
  });
});
