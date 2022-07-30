module.exports = {
  root: true,
  parser: "@typescript-eslint/parser",
  plugins: ["@typescript-eslint"],
  extends: [
    "plugin:@typescript-eslint/recommended",
    "plugin:prettier/recommended",
  ],
  parserOptions: {
    project: "./tsconfig.json",
    ecmaVersion: 2018,
    sourceType: "module",
  },
  rules: {
    "@typescript-eslint/array-type": "off",
    // eslint complains about this:
    // "@typescript-eslint/array-type": ["error", {readonly: "array"}],

    // Awaiting a non-thenable is often a bug and never useful.
    "@typescript-eslint/await-thenable": "error",

    "@typescript-eslint/ban-ts-ignore": "off",

    "@typescript-eslint/explicit-function-return-type": [
      "error",
      {
        // Allows functions that are expressions to avoid declaring a return
        // type.
        allowExpressions: true,
      },
    ],

    // It seems like a drag to have to mark every member as public.
    "@typescript-eslint/explicit-member-accessibility": [
      "error",
      {accessibility: "off"},
    ],

    // This complains when we use ! to strip null or undefined from a type, but
    // we only do this when the type checker hasn't followed the logic fully.
    "@typescript-eslint/no-non-null-assertion": "off",

    // The docs say "Parameter properties can be confusing to those new to
    // TypeScript as they are less explicit than other ways of declaring and
    // initializing class members.". That's not enough for us, given how useful
    // they are.
    "@typescript-eslint/no-parameter-properties": [
      "error",
      {
        allows: ["readonly", "private readonly"],
      },
    ],

    "@typescript-eslint/no-unused-vars": [
      "error",
      {
        vars: "all",
        // It's nice to be able to declare all of the args that an inline
        // function takes, even if we don't use them all.
        args: "none",
      },
    ],

    // This is too stupid to ignore references inside a function to symbols that
    // will be defined by the time that the function executes.
    "@typescript-eslint/no-use-before-define": ["off"],

    // Everyone says that we should use `import foo = require("foo")` to load
    // JS modules but that doesn't work with noImplicitAny.
    "@typescript-eslint/no-var-requires": "off",

    // Likely to be removed from the default.
    // https://github.com/typescript-eslint/typescript-eslint/issues/433
    "@typescript-eslint/prefer-interface": "off",

    // Prevents assigning `this` to other variables on the basis that you might
    // not know about arrow functions. But annoying when you have other reasons
    // to do this.
    "@typescript-eslint/no-this-alias": "off",

    "@typescript-eslint/strict-boolean-expressions": ["error", {}],

    // We fix these automatically with prettier. There's no need to treat them
    // as errors while editing a file.
    "prettier/prettier": "off",

    "@typescript-eslint/ban-ts-comment": [
      "error",
      {"ts-ignore": "allow-with-description"},
    ],

    // The official line is that we shouldn't use `object` as a type. We should
    // fix this.
    "@typescript-eslint/ban-types": "off",
  },
};
