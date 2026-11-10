#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
headers=(-H 'Content-Type: application/json')
if [[ -n "${API_KEY:-}" ]]; then
    headers+=(-H "X-API-Key: $API_KEY")
fi

printf 'Health: '
curl -fsS "$BASE_URL/health"
printf '\n'

payload='{"sources":[{"name":"csv","type":"csv","endpoint":"testdata/input/customers.csv"},{"name":"json","type":"json","endpoint":"testdata/input/customers.json"}],"validation_rules":{"required_fields":["id","city"]},"transform_rules":{"normalize":["city"]},"aggregations":[{"name":"by_city","operation":"count","group_by":["city"]}]}'
created="$(curl -fsS -X POST "$BASE_URL/api/v1/pipelines" "${headers[@]}" -d "$payload")"
job_id="$(printf '%s' "$created" | sed -n 's/.*"job_id":"\([^"]*\)".*/\1/p')"
if [[ -z "$job_id" ]]; then
    echo "Job creation did not return an ID: $created" >&2
    exit 1
fi
printf 'Job ID: %s\n' "$job_id"

path="$BASE_URL/api/v1/pipelines/$job_id"
for i in $(seq 1 60); do
    status="$(curl -fsS "$path/progress" "${headers[@]}")"
    case "$status" in
        *'"state":"COMPLETED"'*) break ;;
        *'"state":"FAILED"'*|*'"state":"CANCELLED"'*) echo "$status" >&2; exit 1 ;;
    esac
    sleep 0.2
done
if [[ "$status" != *'"state":"COMPLETED"'* ]]; then
    echo "Timeout while polling: $status" >&2
    exit 1
fi
printf 'Progress: %s\n' "$status"
results="$(curl -fsS "$path/results" "${headers[@]}")"
failures="$(curl -fsS "$path/errors" "${headers[@]}")"
if [[ "$results" != *'"aggregations":'* || "$results" != *'"records":'* ]]; then
    echo "Missing result fields: $results" >&2
    exit 1
fi
printf 'Errors: %s\n' "$failures"
printf 'Smoke test passed: %s\n' "$job_id"
