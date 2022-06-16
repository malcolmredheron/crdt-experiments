import {TypedValue, value} from "./TypedValue";
import {expect} from "chai";

describe("TypedValue", () => {
  it("roundtrips values", () => {
    class Name extends TypedValue<"Name", string> {}
    const name = Name.create("alice");
    expect(value(name)).equals("alice");
  });

  it("checks types and type names", () => {
    class Name extends TypedValue<"Name", string> {}
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    class Address extends TypedValue<"Address", string> {}
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    class NumberName extends TypedValue<"Name", number> {}

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const name = Name.create("alice");
    // const address: Address = name; // Type names do not match
    // const numberName: NumberName = name; // Value types do not match
  });

  it("compares underlying values", () => {
    class Weight extends TypedValue<"Weight", number> {}
    // At runtime we are actually comparing the raw values.
    expect(Weight.create(1) < Weight.create(2)).equals(true);
    expect(Weight.create(1) > Weight.create(2)).equals(false);
    expect(Weight.create(1) === Weight.create(1)).equals(true);
  });
});
