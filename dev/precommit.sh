#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

npm install
npx tsc --noEmit --project tsconfig.json # check types
npx prettier --write --config .prettierrc "**"
npx eslint --max-warnings=0 "src/**"
npm run testOnce
# Parcel's caching is buggy and will fail when we remove a test file.
# If the build fails, try `rm -rf .parcel-cache/`.
npx parcel build src/mochaInBrowswer/MochaInBrowser.html
echo
echo "All good"
