import {MemoizeInstance, MemoizeStatic} from "./Memoize";
import {expect} from "chai";

describe("Memoize", () => {
  it("MemoizeStatic", () => {
    let result = 0;

    class TestHelper0 {
      @MemoizeStatic
      static staticMethod(object: {}): number {
        return result;
      }
    }
    class TestHelper1 {
      @MemoizeStatic
      static staticMethod(object: {}): number {
        return result;
      }
    }

    const o0 = {};
    const o1 = {};
    expect(TestHelper0.staticMethod(o0)).equals(0);
    result = 1;
    // Hit the cache.
    expect(TestHelper0.staticMethod(o0)).equals(0);
    // Different arg gets a new cache entry.
    expect(TestHelper0.staticMethod(o1)).equals(1);
    // Different class gets a new cache.
    expect(TestHelper1.staticMethod(o0)).equals(1);
  });

  it("MemoizeInstance", () => {
    let result = 0;

    class TestHelper0 {
      @MemoizeInstance
      method(): number {
        return result;
      }

      @MemoizeInstance
      method1(): number {
        return result;
      }
    }

    const o0 = new TestHelper0();
    const o1 = new TestHelper0();
    expect(o0.method()).equals(0);
    result = 1;
    // Hit the cache.
    expect(o0.method()).equals(0);
    // Different arg gets a new cache entry.
    expect(o1.method()).equals(1);
    // Different method gets a new cache.
    expect(o0.method1()).equals(1);
  });
});
