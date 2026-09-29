export MSYS_NO_PATHCONV=1
export AWS_PAGER=""

log() { echo "[$(date -u +%H:%M:%S)] $*"; }

step() {
  echo
  echo "==> $*"
}

native_path() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else echo "$1"; fi
}

stack_output() {
  aws cloudformation describe-stacks --stack-name "${STACK_NAME}" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

print_stack_failures() {
  aws cloudformation describe-stack-events --stack-name "${STACK_NAME}" \
    --query "StackEvents[?contains(ResourceStatus, 'FAILED')].[Timestamp,LogicalResourceId,ResourceStatus,ResourceStatusReason]" \
    --output table 2>/dev/null || true
}

env_header() {
  local name
  for name in "$@"; do
    printf 'export %s=%q\n' "${name}" "${!name}"
  done
}

wait_for_ssm() {
  local timeout="$1"; shift
  local ids online deadline=$((SECONDS + timeout))
  ids=$(IFS=,; echo "$*")
  while :; do
    online=$(aws ssm describe-instance-information \
      --filters "Key=InstanceIds,Values=${ids}" \
      --query "length(InstanceInformationList[?PingStatus=='Online'])" --output text 2>/dev/null || echo 0)
    log "SSM agents online: ${online}/$#"
    [ "${online}" = "$#" ] && return 0
    if [ "${SECONDS}" -ge "${deadline}" ]; then
      log "ERROR: SSM agents did not come online within ${timeout}s"
      return 1
    fi
    sleep 10
  done
}

ssm_send() {
  local instance="$1" timeout="$2" body="$3" arg="${4:-}" b64 params
  [[ "${arg}" =~ ^[A-Za-z0-9._-]*$ ]] || { echo "ssm_send: unsafe argument '${arg}'" >&2; return 1; }

  b64=$(printf '%s\n' "${body}" | base64 | tr -d '\n')
  params=$(printf '{"executionTimeout":["%s"],"commands":["f=$(mktemp)","echo %s | base64 -d > \\"$f\\"","bash \\"$f\\" %s; rc=$?","rm -f \\"$f\\"","exit $rc"]}' \
    "${timeout}" "${b64}" "${arg}")

  aws ssm send-command \
    --instance-ids "${instance}" \
    --document-name AWS-RunShellScript \
    --parameters "${params}" \
    --query "Command.CommandId" --output text
}

ssm_wait() {
  local command_id="$1" instance="$2" deadline=$(($3 + SECONDS + 120)) status
  while :; do
    status=$(aws ssm get-command-invocation --command-id "${command_id}" --instance-id "${instance}" \
      --query "Status" --output text 2>/dev/null || echo Pending)
    case "${status}" in
      Pending|InProgress|Delayed)
        if [ "${SECONDS}" -ge "${deadline}" ]; then status="TimedOut (waiting locally)"; break; fi
        sleep 5 ;;
      *) break ;;
    esac
  done

  echo "---- ${instance}: ${status}"
  aws ssm get-command-invocation --command-id "${command_id}" --instance-id "${instance}" \
    --query "StandardOutputContent" --output text 2>/dev/null | sed 's/^/    /'
  if [ "${status}" != "Success" ]; then
    echo "    -- stderr (last 40 lines) --"
    aws ssm get-command-invocation --command-id "${command_id}" --instance-id "${instance}" \
      --query "StandardErrorContent" --output text 2>/dev/null | tail -n 40 | sed 's/^/    /'
    return 1
  fi
}

ssm_run() {
  local timeout="$1" body="$2" i id failed=0; shift 2
  local instances=("$@") ids=()
  for i in "${instances[@]}"; do
    id=$(ssm_send "${i}" "${timeout}" "${body}") || { failed=1; id=""; }
    ids+=("${id}")
  done
  for i in "${!instances[@]}"; do
    if [ -z "${ids[$i]}" ]; then echo "---- ${instances[$i]}: send-command failed"; continue; fi
    ssm_wait "${ids[$i]}" "${instances[$i]}" "${timeout}" || failed=1
  done
  return "${failed}"
}

check() {
  local name="$1" timeout="$2" body="$3"; shift 3
  step "CHECK: ${name}"
  if ssm_run "${timeout}" "${REMOTE_ENV}"$'\n'"BIN=/opt/kafka/bin"$'\n'"${body}" "$@"; then
    RESULTS+=("PASS  ${name}")
  else
    RESULTS+=("FAIL  ${name}")
    summary
    exit 1
  fi
}

summary() {
  step "Validation summary (topic ${TOPIC})"
  printf '  %s\n' "${RESULTS[@]}"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
      echo "## Kafka validation: ${STACK_NAME}"
      echo
      echo "| Result | Check |"
      echo "|---|---|"
      for r in "${RESULTS[@]}"; do echo "| ${r%%  *} | ${r#*  } |"; done
    } >> "${GITHUB_STEP_SUMMARY}"
  fi
}

main() {
  set -euo pipefail
  : "${STACK_NAME:?set STACK_NAME to the stack to validate}"
  MESSAGE_COUNT="${MESSAGE_COUNT:-100}"
  RUN_ID="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
  TOPIC="e2e-${RUN_ID}"
  RESULTS=()

  IFS=, read -r -a BROKERS <<< "$(stack_output BrokerInstanceIds)"
  BOOTSTRAP=$(stack_output BootstrapServers)
  [ "${#BROKERS[@]}" -eq 3 ] || { log "ERROR: expected 3 broker ids in stack outputs, got '${BROKERS[*]}'"; exit 1; }

  REMOTE_ENV=$(env_header BOOTSTRAP TOPIC MESSAGE_COUNT RUN_ID)

  check "kafka service active on all brokers" 60 '
if systemctl is-active --quiet kafka; then
  echo "OK: kafka active on $(hostname)"
else
  echo "FAIL: kafka not active on $(hostname)"
  journalctl -u kafka -n 30 --no-pager
  exit 1
fi' "${BROKERS[@]}"

  check "KRaft quorum (3 voters, leader) and 3 brokers registered" 240 '
for _ in $(seq 1 36); do
  STATUS=$($BIN/kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" describe --status 2>/dev/null)
  REPLICATION=$($BIN/kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" describe --replication 2>/dev/null)
  LEADER=$(echo "$STATUS" | awk -F": *" "/^LeaderId/ {print \$2}")
  VOTERS=$(echo "$REPLICATION" | grep -cE "(Leader|Follower)[[:space:]]*$")
  BROKERS=$($BIN/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" 2>/dev/null | grep -c "(id: ")
  if [ -n "$LEADER" ] && [ "$LEADER" != "-1" ] && [ "$VOTERS" -eq 3 ] && [ "$BROKERS" -eq 3 ]; then
    echo "$REPLICATION"
    echo "OK: leader=$LEADER voters=$VOTERS brokers=$BROKERS"
    exit 0
  fi
  sleep 5
done
echo "$STATUS"
echo "$REPLICATION"
echo "FAIL: leader=${LEADER:-none} voters=$VOTERS brokers=$BROKERS"
exit 1' "${BROKERS[0]}"

  check "topic ${TOPIC} with RF 3, all replicas in sync" 120 '
$BIN/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic "$TOPIC" \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2 || exit 1
for _ in $(seq 1 12); do
  DESC=$($BIN/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC")
  PARTITIONS=$(echo "$DESC" | grep -c "Partition: ")
  NOT_IN_SYNC=$(echo "$DESC" | grep "Partition: " \
    | awk "{for (i = 1; i <= NF; i++) if (\$i == \"Isr:\") print \$(i + 1)}" \
    | awk -F, "NF != 3" | wc -l)
  if [ "$PARTITIONS" -eq 3 ] && [ "$NOT_IN_SYNC" -eq 0 ]; then
    echo "$DESC"
    echo "OK: $PARTITIONS partitions, every ISR has 3 replicas"
    exit 0
  fi
  sleep 5
done
echo "$DESC"
echo "FAIL: $NOT_IN_SYNC of $PARTITIONS partitions are not fully in sync"
exit 1' "${BROKERS[0]}"

  check "produce ${MESSAGE_COUNT} messages from broker-0" 180 '
seq 1 "$MESSAGE_COUNT" | sed "s/^/$RUN_ID-/" | $BIN/kafka-console-producer.sh \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --producer-property acks=all --producer-property enable.idempotence=true || exit 1
PRODUCED=$($BIN/kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  | awk -F: "{sum += \$3} END {print sum + 0}")
echo "PRODUCED=$PRODUCED expected=$MESSAGE_COUNT"
[ "$PRODUCED" -eq "$MESSAGE_COUNT" ]' "${BROKERS[0]}"

  check "consume ${MESSAGE_COUNT} messages from broker-1" 180 '
OUT=$(mktemp)
$BIN/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --group "e2e-$RUN_ID" --from-beginning \
  --max-messages "$MESSAGE_COUNT" --timeout-ms 60000 > "$OUT" 2>/dev/null
CONSUMED=$(grep -c "^$RUN_ID-" "$OUT")
UNIQUE=$(grep "^$RUN_ID-" "$OUT" | sort -u | wc -l)
MISSING=$(comm -23 <(seq 1 "$MESSAGE_COUNT" | sed "s/^/$RUN_ID-/" | sort) <(sort -u "$OUT") | wc -l)
rm -f "$OUT"
echo "CONSUMED=$CONSUMED UNIQUE=$UNIQUE MISSING=$MISSING expected=$MESSAGE_COUNT"
[ "$CONSUMED" -eq "$MESSAGE_COUNT" ] && [ "$UNIQUE" -eq "$MESSAGE_COUNT" ] && [ "$MISSING" -eq 0 ]' "${BROKERS[1]}"

  summary
  log "All checks passed"
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi
