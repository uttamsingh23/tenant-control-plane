#!/usr/bin/env bash
# Publishes duplicate, out-of-order, conflicting, unknown-task and malformed
# progress updates straight to RabbitMQ (management HTTP API) for a tenant
# that has finished its deploy, then checks that none of them changed it.
#
#   scripts/replay.sh
#
# Needs curl and jq, a running stack and a worker at --fail-rate=0.
# RABBITMQ_USER / RABBITMQ_PASSWORD come from the environment (make exports
# .env; run it as `make replay`).
set -euo pipefail

API_URL=${API_URL:-http://localhost:8080}
MGMT_URL=${MGMT_URL:-http://localhost:${RABBITMQ_MANAGEMENT_PORT:-15672}}
: "${RABBITMQ_USER:?set RABBITMQ_USER (run via make replay, which loads .env)}"
: "${RABBITMQ_PASSWORD:?set RABBITMQ_PASSWORD (run via make replay, which loads .env)}"

die() { echo "FAIL: $*" >&2; exit 1; }
mgmt() { curl -fsS -u "$RABBITMQ_USER:$RABBITMQ_PASSWORD" -H 'Content-Type: application/json' "$@"; }

dead_letters() {
  mgmt "$MGMT_URL/api/queues/%2F/tenant.dead-letter" | jq .messages
}

slug="replay-$(date +%s)-$RANDOM"
tenant=$(curl -fsS -XPOST "$API_URL/v1/tenants" -H 'Content-Type: application/json' \
  -d "{\"slug\":\"$slug\",\"name\":\"Replay demo\"}" | jq -r .tenant.id)
deadline=$((SECONDS + 30))
until [[ $(curl -fsS "$API_URL/v1/tenants/$tenant" | jq -r .status) == active ]]; do
  ((SECONDS < deadline)) || die "tenant $tenant did not become active (is the worker at --fail-rate=0?)"
  sleep 0.5
done
task=$(curl -fsS "$API_URL/v1/tasks?tenant_id=$tenant" | jq -r '.items[0].id')
before=$(curl -fsS "$API_URL/v1/tenants/$tenant" | jq -c '{status, version}')
dl_before=$(dead_letters)
echo "tenant $tenant is $before, deploy task $task"

# publish <message_id> <task_id> <previous_status> <status> [reason]
publish() {
  jq -nc --arg id "$1" --arg task "$2" --arg tenant "$tenant" --arg prev "$3" --arg st "$4" --arg reason "${5:-}" '
    {schema_version:1, message_id:$id, task_id:$task, tenant_id:$tenant, task_type:"deploy",
     previous_status:$prev, status:$st, occurred_at:"2026-01-01T00:00:00.000Z"}
    + (if $reason == "" then {} else {reason:$reason} end)
    | {properties:{message_id:$id, content_type:"application/json", delivery_mode:2},
       routing_key:("task_update." + $st), payload:tojson, payload_encoding:"string"}' |
    mgmt -XPOST "$MGMT_URL/api/exchanges/%2F/tenant.task-updates/publish" -d @- | jq -e '.routed' >/dev/null ||
    die "message $1 was not routed"
}

echo "duplicate:   the worker's own outcome again (same message_id)"
publish "$task:outcome" "$task" in_progress done
publish "$task:outcome" "$task" in_progress done
echo "out of order: in_progress after done, new message_id"
publish "manual-stale-$RANDOM" "$task" accepted in_progress
echo "conflict:    failed after done"
publish "manual-conflict-$RANDOM" "$task" in_progress failed "late failure"
echo "unknown task (dead-lettered)"
publish "manual-ghost-$RANDOM" "$(uuidgen | tr '[:upper:]' '[:lower:]')" in_progress done
echo "malformed body (dead-lettered)"
mgmt -XPOST "$MGMT_URL/api/exchanges/%2F/tenant.task-updates/publish" \
  -d '{"properties":{"message_id":"garbage-1"},"routing_key":"task_update.x","payload":"not json","payload_encoding":"string"}' >/dev/null

# Queue counts in the management API refresh every few seconds, so poll.
deadline=$((SECONDS + 20))
until (($(dead_letters) >= dl_before + 2)) || ((SECONDS >= deadline)); do sleep 1; done

after=$(curl -fsS "$API_URL/v1/tenants/$tenant" | jq -c '{status, version}')
task_status=$(curl -fsS "$API_URL/v1/tasks/$task" | jq -r .status)
dl_after=$(dead_letters)
echo "tenant now $after, task $task_status, dead letters $dl_before -> $dl_after"
[[ $after == "$before" ]] || die "tenant changed from $before to $after"
[[ $task_status == done ]] || die "task regressed to $task_status"
((dl_after >= dl_before + 2)) || die "expected the unknown-task and malformed messages in tenant.dead-letter"
echo "OK: duplicates and stale updates changed nothing; poison messages were dead-lettered and the consumer kept going"
