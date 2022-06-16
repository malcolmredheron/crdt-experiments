#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

npm install
npx tsc --noEmit --project tsconfig.json # check types
npx prettier --write --config .prettierrc "**"
npx eslint --max-warnings=0 "src/**"
npm run testOnce
echo
echo "All good"
