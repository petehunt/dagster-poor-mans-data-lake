#!/bin/bash
set -euo pipefail
npx --yes concurrently -n daemon,init 'localstack start' 'echo "Setting up localstack. One moment..." && npx --yes wait-port -o silent 4566 && awslocal s3 mb s3://datalake && echo "Ready to go"'