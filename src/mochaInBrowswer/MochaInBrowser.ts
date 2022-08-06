import * as mocha from "mocha";
import {touch} from "../helper/Shared.testing";

// This installs custom formatters so that browsers (or just chrome?) can show
// prelude types readably.
require("../../node_modules/prelude-ts/dist/src/chrome_dev_tools_formatters");

const browserMocha = mocha as unknown as BrowserMocha;
browserMocha.setup({ui: "bdd"});

// Get parceljs to glob all of our tests together
// @ts-ignore TypeScript doesn't understand wildcard includes
touch(require("../**/*.test.ts"));

browserMocha.run();
