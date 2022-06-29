import * as mocha from "mocha";
import {touch} from "../helper/Shared.testing";

const browserMocha = mocha as unknown as BrowserMocha;
browserMocha.setup({ui: "bdd"});

// Get parceljs to glob all of our tests together
// @ts-ignore TypeScript doesn't understand wildcard includes
touch(require("../**/*.test.ts"));

browserMocha.run();
