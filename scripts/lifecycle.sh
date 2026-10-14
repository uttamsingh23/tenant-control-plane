#!/usr/bin/env bash
# Walks one tenant through its lifecycle against a running stack and fails
# loudly if any step ends in the wrong state.
#
#   scripts/lifecycle.sh           # worker at --fail-rate=0: create -> active -> PATCH -> active -> DELETE -> destroyed
#   scripts/lifecycle.sh failed    # worker at --fail-rate=1: create -> failed -> DELETE -> failed
#
# Needs curl and jq. API_URL defaults to http://localhost:8080.
set -euo pipefail

API_URL=${API_URL:-http://localhost:8080}
EXPECT=${1:-active}
TIMEOUT_S=${TIMEOUT_S:-30}

case "$EXPECT" in
active | failed) ;;
*) echo "usage: $0 [active|failed]" >&2; exit 2 ;;
esac

die() { echo "FAIL: $*" >&2; exit 1; }

# wait_for <tenant id> <status>: polls the tenant until it reaches status.
wait_for() {
  local deadline=$((SECONDS + TIMEOUT_S)) status
  while ((SECONDS < deadline)); do
    status=$(curl -fsS "$API_URL/v1/tenants/$1" | jq -r .status)
    [[ $status == "$2" ]] && return 0
    sleep 0.5
  done
  die "tenant $1 is $status, expected $2 after ${TIMEOUT_S}s"
}

show() {
  curl -fsS "$API_URL/v1/tenants/$1" | jq -c '{slug, status, version}'
  curl -fsS "$API_URL/v1/tasks?tenant_id=$1" | jq -c '.items[] | {type, status}'
}

slug="demo-$(date +%s)-$RANDOM"
echo "== POST /v1/tenants slug=$slug"
created=$(curl -fsS -XPOST "$API_URL/v1/tenants" -H 'Content-Type: application/json' \
  -d "{\"slug\":\"$slug\",\"name\":\"Demo\"}")
echo "$created" | jq -c '{tenant: {status: .tenant.status, version: .tenant.version}, task: {type: .task.type, status: .task.status}}'
id=$(echo "$created" | jq -r .tenant.id)

wait_for "$id" "$EXPECT"
show "$id"

if [[ $EXPECT == active ]]; then
  version=$(curl -fsS "$API_URL/v1/tenants/$id" | jq .version)
  echo "== PATCH name, version=$version"
  curl -fsS -XPATCH "$API_URL/v1/tenants/$id" -H 'Content-Type: application/json' \
    -d "{\"name\":\"Demo renamed\",\"version\":$version}" | jq -c '{tenant: .tenant.status, task: .task.type}'
  wait_for "$id" active
  show "$id"

  echo "== DELETE"
  curl -fsS -XDELETE "$API_URL/v1/tenants/$id" | jq -c '{tenant: .tenant.status, task: .task.type}'
  wait_for "$id" destroyed
  show "$id"

  echo "== POST the same slug again"
  code=$(curl -sS -o /dev/null -w '%{http_code}' -XPOST "$API_URL/v1/tenants" -H 'Content-Type: application/json' \
    -d "{\"slug\":\"$slug\",\"name\":\"Again\"}")
  [[ $code == 409 ]] || die "re-using a destroyed slug returned $code, expected 409"
  echo "409 tenant_already_exists"
else
  echo "== DELETE the failed tenant (worker still fails, so it ends failed again)"
  curl -fsS -XDELETE "$API_URL/v1/tenants/$id" | jq -c '{tenant: .tenant.status, task: .task.type}'
  # The tenant is failed both before and after the destroy, so wait on the
  # destroy task (the newest task; lists are oldest first).
  deadline=$((SECONDS + TIMEOUT_S))
  until [[ $(curl -fsS "$API_URL/v1/tasks?tenant_id=$id" | jq -r '.items[-1] | "\(.type)/\(.status)"') == destroy/failed ]]; do
    ((SECONDS < deadline)) || die "destroy task did not fail within ${TIMEOUT_S}s"
    sleep 0.5
  done
  wait_for "$id" failed
  show "$id"
fi

echo "OK: tenant $id"
