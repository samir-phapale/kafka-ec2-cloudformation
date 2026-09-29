set -euo pipefail

NODE_ID="${1:?usage: setup-kafka.sh <node-id>}"
: "${KAFKA_VERSION:?}" "${SCALA_VERSION:?}" "${KAFKA_PORT:?}" "${KAFKA_CONTROLLER_PORT:?}"
: "${DNS_ZONE:?}" "${QUORUM_VOTERS:?}" "${CLUSTER_ID:?}"

KAFKA_DIST="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_HOME=/opt/kafka
CONFIG=/etc/kafka/server.properties
DATA_DIR=/var/lib/kafka/data

IMDS_TOKEN=$(curl -sf -X PUT http://169.254.169.254/latest/api/token \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
AZ=$(curl -sf -H "X-aws-ec2-metadata-token: ${IMDS_TOKEN}" \
  http://169.254.169.254/latest/meta-data/placement/availability-zone)
ADVERTISED_HOST="broker-${NODE_ID}.${DNS_ZONE}"
echo "node.id=${NODE_ID} az=${AZ} host=${ADVERTISED_HOST}"

dnf install -y -q java-17-amazon-corretto-headless tar

id -u kafka &>/dev/null || useradd -r -m -d /var/lib/kafka -s /sbin/nologin kafka

if [ ! -d "/opt/${KAFKA_DIST}" ]; then
  if curl -fsSL "https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_DIST}.tgz" -o /tmp/kafka.tgz; then
    echo "Downloaded ${KAFKA_DIST} from dlcdn.apache.org"
  else
    echo "${KAFKA_DIST} not on dlcdn.apache.org, trying archive.apache.org"
    curl -fsSL --retry 5 --retry-delay 10 \
      "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_DIST}.tgz" -o /tmp/kafka.tgz
  fi
  tar -xzf /tmp/kafka.tgz -C /opt
  rm -f /tmp/kafka.tgz
fi
ln -sfn "/opt/${KAFKA_DIST}" "${KAFKA_HOME}"

mkdir -p /etc/kafka "${DATA_DIR}" "/opt/${KAFKA_DIST}/logs"
chown -R kafka:kafka "/opt/${KAFKA_DIST}" /var/lib/kafka

cat > "${CONFIG}" <<EOF
process.roles=broker,controller
node.id=${NODE_ID}
controller.quorum.voters=${QUORUM_VOTERS}
listeners=PLAINTEXT://0.0.0.0:${KAFKA_PORT},CONTROLLER://0.0.0.0:${KAFKA_CONTROLLER_PORT}
advertised.listeners=PLAINTEXT://${ADVERTISED_HOST}:${KAFKA_PORT}
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
broker.rack=${AZ}
log.dirs=${DATA_DIR}
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
unclean.leader.election.enable=false
auto.create.topics.enable=false
EOF
chown kafka:kafka "${CONFIG}"

MEM_MB=$(awk '/MemTotal/ {print int($2 / 1024)}' /proc/meminfo)
HEAP_MB=$((MEM_MB / 2))
[ "${HEAP_MB}" -lt 512 ] && HEAP_MB=512
[ "${HEAP_MB}" -gt 6144 ] && HEAP_MB=6144

cat > /etc/systemd/system/kafka.service <<EOF
[Unit]
Description=Apache Kafka (KRaft, node ${NODE_ID})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=kafka
Environment="KAFKA_HEAP_OPTS=-Xms${HEAP_MB}m -Xmx${HEAP_MB}m"
ExecStart=${KAFKA_HOME}/bin/kafka-server-start.sh ${CONFIG}
Restart=on-failure
RestartSec=10
LimitNOFILE=100000
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
EOF

if [ -f "${DATA_DIR}/meta.properties" ]; then
  EXISTING_ID=$(awk -F= '/^cluster.id=/ {print $2}' "${DATA_DIR}/meta.properties")
  if [ "${EXISTING_ID}" != "${CLUSTER_ID}" ]; then
    echo "ERROR: ${DATA_DIR} is formatted for cluster ${EXISTING_ID}, expected ${CLUSTER_ID}"
    exit 1
  fi
  echo "Storage already formatted"
else
  sudo -u kafka "${KAFKA_HOME}/bin/kafka-storage.sh" format -t "${CLUSTER_ID}" -c "${CONFIG}"
fi

for n in 0 1 2; do
  for _ in $(seq 1 30); do
    getent hosts "broker-${n}.${DNS_ZONE}" >/dev/null && break
    sleep 2
  done
  getent hosts "broker-${n}.${DNS_ZONE}" >/dev/null || { echo "ERROR: broker-${n}.${DNS_ZONE} does not resolve"; exit 1; }
done

systemctl daemon-reload
systemctl enable kafka
systemctl restart kafka

for _ in $(seq 1 60); do
  if "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" --bootstrap-server "localhost:${KAFKA_PORT}" >/dev/null 2>&1; then
    echo "SETUP_OK node.id=${NODE_ID} az=${AZ} host=${ADVERTISED_HOST} heap=${HEAP_MB}m"
    exit 0
  fi
  sleep 5
done

echo "ERROR: broker ${NODE_ID} not ready after 5 minutes"
journalctl -u kafka -n 40 --no-pager || true
exit 1
