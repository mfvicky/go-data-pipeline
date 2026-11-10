#!/usr/bin/env bash
set -euo pipefail
: "${API_KEY:?export API_KEY before running}"
mkdir -p prometheus-secrets
umask 077
printf '%s' "$API_KEY" > prometheus-secrets/api_key
echo 'Prometheus key prepared locally (not included in archives).'
