#!/bin/bash
set -e

# Clone contracts into the path that replace directive expects
rm -rf ../kairos-contracts
git clone --depth 1 https://github.com/kairos-development/kairos-contracts.git ../kairos-contracts

echo "Dependencies cloned to ../kairos-contracts"
