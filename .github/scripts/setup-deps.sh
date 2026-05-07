#!/bin/bash
set -e

DEPS_DIR=$(mktemp -d)

git clone --depth 1 https://github.com/kairos-development/kairos-contracts.git "$DEPS_DIR/kairos-contracts"

sed -i "s|../kairos-contracts|$DEPS_DIR/kairos-contracts|g" go.mod

echo "Dependencies cloned to $DEPS_DIR"
echo "go.mod replace:"
grep "replace" go.mod

go mod tidy
