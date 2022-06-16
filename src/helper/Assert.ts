// This is a standardized way of signalling a non-recoverable error so that it's
// easy to put a breakpoint here.
//
// Problems with other approaches:
// - It takes many breakpoints to trap all possible errors
// - The debugger's line numbers are often sufficiently off, or the throw is on
//   the same line as a test, so that it's impossible to set a breakpoint.
export class AssertFailed extends Error {
  constructor(message: string, readonly args?: {[name: string]: unknown}) {
    super(message);
    debugger;
  }
}
