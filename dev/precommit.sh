#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

echo "npm"; npm install --silent
echo "tsc"; npx tsc --noEmit --incremental --project tsconfig.json # check types
echo "prettier"; npx prettier --write --config .prettierrc --cache --loglevel=warn "**"
# set TIMING=1 to get a table showing how long different rules took.
echo "eslint"; npx eslint --max-warnings=0 "src/**"
echo "testOnce"; npm run test
# Parcel's caching is buggy and will fail when we remove a test file.
# If the build fails, try `rm -rf .parcel-cache/`.
echo "parcel"; npx parcel build --log-level warn src/mochaInBrowswer/MochaInBrowser.html
echo
echo "All good"
