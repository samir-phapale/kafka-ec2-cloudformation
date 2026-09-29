#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=validate-kafka.sh
source "${SCRIPT_DIR}/validate-kafka.sh"

ENVIRONMENT="${1:-dev}"
TEMPLATE_FILE="${ROOT_DIR}/template.yaml"
PARAMS_FILE="${ROOT_DIR}/parameters/${ENVIRONMENT}.json"
[ -f "${PARAMS_FILE}" ] || { echo "No parameter file at ${PARAMS_FILE}"; exit 1; }

PROJECT_NAME=$(sed -n 's/.*"ProjectName": *"\([^"]*\)".*/\1/p' "${PARAMS_FILE}")
PROJECT_NAME="${PROJECT_NAME:-kafka-on-ec2}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
STACK_NAME="${STACK_NAME:-${PROJECT_NAME}-cfn-${ENVIRONMENT}-${RUN_ID}}"
KEEP_STACK="${KEEP_STACK:-0}"
export STACK_NAME RUN_ID

if [ "${#STACK_NAME}" -gt 50 ]; then
  echo "STACK_NAME '${STACK_NAME}' is longer than 50 characters"
  exit 1
fi

STACK_TOUCHED=0

cleanup() {
  local rc=$?
  trap - EXIT INT TERM

  if [ "${STACK_TOUCHED}" = 1 ]; then
    if [ "${KEEP_STACK}" = 1 ]; then
      step "Keeping ${STACK_NAME}"
      echo "  validate: STACK_NAME=${STACK_NAME} bash scripts/validate-kafka.sh"
      echo "  delete:   aws cloudformation delete-stack --stack-name ${STACK_NAME}"
    else
      step "Deleting ${STACK_NAME}"
      if aws cloudformation delete-stack --stack-name "${STACK_NAME}" &&
         aws cloudformation wait stack-delete-complete --stack-name "${STACK_NAME}"; then
        log "Stack deleted"
      else
        log "ERROR: failed to delete stack"
        print_stack_failures
        [ "${rc}" -eq 0 ] && rc=1
      fi
    fi
  fi

  if [ "${rc}" -eq 0 ]; then
    step "PASSED ${STACK_NAME}"
  else
    step "FAILED ${STACK_NAME} (exit ${rc})"
  fi
  exit "${rc}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

log "Account: $(aws sts get-caller-identity --query Account --output text)  Region: ${AWS_REGION:-$(aws configure get region 2>/dev/null || echo '?')}"
log "Stack:   ${STACK_NAME}"

step "Deploy"
STACK_TOUCHED=1
if ! aws cloudformation deploy \
  --stack-name "${STACK_NAME}" \
  --template-file "$(native_path "${TEMPLATE_FILE}")" \
  --parameter-overrides "file://$(native_path "${PARAMS_FILE}")" \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset \
  --tags Project="${PROJECT_NAME}" Environment="${ENVIRONMENT}" RunId="${RUN_ID}" ManagedBy=cloudformation; then
  print_stack_failures
  exit 1
fi

IFS=, read -r -a BROKERS <<< "$(stack_output BrokerInstanceIds)"
log "Brokers: ${BROKERS[*]}"

step "Wait for instances"
aws ec2 wait instance-status-ok --instance-ids "${BROKERS[@]}"
log "Instance status checks OK"
wait_for_ssm 600 "${BROKERS[@]}"

step "Install Kafka"
KAFKA_VERSION=$(stack_output KafkaVersion)
SCALA_VERSION=$(stack_output ScalaVersion)
KAFKA_PORT=$(stack_output KafkaPort)
KAFKA_CONTROLLER_PORT=$(stack_output KafkaControllerPort)
DNS_ZONE=$(stack_output DnsZoneName)
QUORUM_VOTERS=$(stack_output QuorumVoters)
CLUSTER_ID=$(stack_output KafkaClusterId)
log "Kafka ${KAFKA_VERSION} (Scala ${SCALA_VERSION}), cluster id ${CLUSTER_ID}"

SETUP_BODY="$(env_header KAFKA_VERSION SCALA_VERSION KAFKA_PORT KAFKA_CONTROLLER_PORT DNS_ZONE QUORUM_VOTERS CLUSTER_ID)
$(cat "${SCRIPT_DIR}/setup-kafka.sh")"

SETUP_COMMANDS=()
for node_id in "${!BROKERS[@]}"; do
  command_id=$(ssm_send "${BROKERS[$node_id]}" 1800 "${SETUP_BODY}" "${node_id}") ||
    { log "ERROR: could not send setup to ${BROKERS[$node_id]}"; exit 1; }
  SETUP_COMMANDS+=("${command_id}")
done
SETUP_FAILED=0
for node_id in "${!BROKERS[@]}"; do
  ssm_wait "${SETUP_COMMANDS[$node_id]}" "${BROKERS[$node_id]}" 1800 || SETUP_FAILED=1
done
[ "${SETUP_FAILED}" -eq 0 ] || { log "ERROR: Kafka setup failed"; exit 1; }

step "Validate"
bash "${SCRIPT_DIR}/validate-kafka.sh"
