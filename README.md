# Installation

## For Linux (via [Nix](https://github.com/NixOS/nix))

The [default.nix](./default.nix) file declaratively defines the development environment. Load this environment by running [nix-shell](https://nixos.org/nixos/nix-pills/developing-with-nix-shell.html) in the root source directory: `nix-shell .`

`npm` and `npx` should now be available in the shell environment.

# Development

After cloning or updating, and before committing:

- `dev/precommit.sh`

To run all tests on the command line (and rerun when files change):

- `npm test`

To run tests in the browser:

- `num run testInBrowser`

## Upgrading

Upgrade nix packages: Follow the instructions in default.nix

Upgrade npm packages:
- `npx npm-check-updates` to check for updates
- `npx npm-check-updates -u` to take all updates

## Intelij

Settings
- Editor
  - Inspections
    - Javascript and Typescript
      - Async code and promises: enable all and set to error (we don't know how
        make eslint check for forgetting to await a promise)
