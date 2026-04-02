#!/bin/bash
set -e

echo "Starting YARN NodeManager..."
yarn --daemon start nodemanager

# Keep container running
tail -f /dev/null