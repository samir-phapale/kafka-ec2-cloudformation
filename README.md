# kafka-ec2-cloudformation

3-node Kafka cluster (KRaft, combined broker/controller) on EC2, one node per AZ in private subnets. Brokers are reached through SSM only.

## Files

- `template.yaml` - VPC, subnets, NAT, security group, IAM role, Route 53 private zone, 3 instances
- `parameters/dev.json` - parameters for dev
- `scripts/run-e2e.sh` - deploys the stack, installs Kafka, runs the checks, deletes the stack
- `scripts/setup-kafka.sh` - runs on each broker via SSM
- `scripts/validate-kafka.sh` - cluster checks and produce/consume test

## Usage

Needs bash and AWS CLI v2. The region must have at least 3 AZs.

```bash
bash scripts/run-e2e.sh dev
```

A run takes around 15-20 minutes. The stack is deleted when the script exits unless `KEEP_STACK=1` is set.

| Variable | Default |
|---|---|
| `KEEP_STACK` | `0` |
| `MESSAGE_COUNT` | `100` |
| `STACK_NAME` | `kafka-on-ec2-cfn-<env>-<RUN_ID>` (max 50 chars) |
| `RUN_ID` | UTC timestamp |

To re-run the checks against a kept stack:

```bash
STACK_NAME=<stack> bash scripts/validate-kafka.sh
```

## Checks

- kafka service active on all brokers
- quorum has 3 voters and a leader, 3 brokers registered
- test topic with RF 3 and full ISR
- produce N messages from broker-0 with `acks=all`
- consume from broker-1, no missing or duplicate messages

## Kafka config

- replication factor 3, `min.insync.replicas=2`
- `unclean.leader.election.enable=false`
- `auto.create.topics.enable=false`
- `broker.rack` set to the AZ
- heap is half of RAM, between 512 MB and 6 GB

Listeners are PLAINTEXT; ports are only open between brokers in the security group.

## CI

PRs run cfn-lint, shellcheck and `validate-template`. Pushes to `main` and manual runs also run the e2e job.

Uses the `AWS_ROLE_ARN` secret and `AWS_REGION` variable. The role needs Route 53 permissions for the private hosted zone:

- `route53:CreateHostedZone`, `DeleteHostedZone`, `GetHostedZone`, `ListHostedZones`, `ListHostedZonesByName`
- `route53:ChangeResourceRecordSets`, `ListResourceRecordSets`, `GetChange`
- `route53:AssociateVPCWithHostedZone`, `DisassociateVPCFromHostedZone`
- `route53:ChangeTagsForResource`, `ListTagsForResource`

## Debugging

Run with `KEEP_STACK=1`, then:

```bash
aws ssm start-session --target <instance-id>
journalctl -u kafka
```
