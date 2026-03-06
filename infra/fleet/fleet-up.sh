#!/usr/bin/env bash
set -euo pipefail

# Provision fleet workers: snapshot the dev EBS volume, then terraform apply
# to create one instance + EBS per worker.
#
# Usage:
#   fleet-up.sh <state1> [state2 ...]     # production workers from state.env
#   fleet-up.sh --test                     # 2 tiny workers for smoke testing

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INFRA_DIR="$SCRIPT_DIR/.."
FLEET_DIR="$SCRIPT_DIR"

# ---------------------------------------------------------------------------
# AWS credentials (same pattern as dev-setup.sh)
# ---------------------------------------------------------------------------
CONFIG_FILE="$REPO_ROOT/.secrets/aws-sso-config.sh"
if [ -f "$CONFIG_FILE" ]; then
  # shellcheck source=/dev/null
  . "$CONFIG_FILE"
fi

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-west-2}"

export_aws_creds() {
  eval "$(aws configure export-credentials --format env 2>/dev/null)"
}
if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
  if ! export_aws_creds || [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
    echo "Could not export AWS credentials. Run 'just aws' first." >&2
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
TEST_MODE=false
STATES=()
for arg in "$@"; do
  case "$arg" in
  --test) TEST_MODE=true ;;
  *) STATES+=("$arg") ;;
  esac
done

# ---------------------------------------------------------------------------
# Build workers JSON map: { "state-utility": "instance_type_override", ... }
# ---------------------------------------------------------------------------
WORKERS_JSON="{"
FIRST=true
add_worker() {
  local key="$1" type="$2"
  if [ "$FIRST" = true ]; then FIRST=false; else WORKERS_JSON+=","; fi
  WORKERS_JSON+="\"$key\":\"$type\""
}

if [ "$TEST_MODE" = true ]; then
  add_worker "test-worker-0" "t3.micro"
  add_worker "test-worker-1" "t3.micro"
  echo "Fleet up (TEST MODE): 2 t3.micro workers"
else
  if [ ${#STATES[@]} -eq 0 ]; then
    echo "Usage: fleet-up.sh [--test] <state1> [state2 ...]" >&2
    exit 1
  fi

  HP_RATES_DIR="$REPO_ROOT/rate_design/hp_rates"
  WORKER_COUNT=0
  for state in "${STATES[@]}"; do
    STATE_ENV="$HP_RATES_DIR/$state/state.env"
    if [ ! -f "$STATE_ENV" ]; then
      echo "Error: state.env not found at $STATE_ENV" >&2
      exit 1
    fi

    UTILITIES=$(grep '^UTILITIES=' "$STATE_ENV" | cut -d= -f2)
    IFS=',' read -ra utils <<<"$UTILITIES"
    for util in "${utils[@]}"; do
      if [ "$util" = "coned" ]; then
        add_worker "${state}-${util}" "m7i.2xlarge"
      else
        add_worker "${state}-${util}" ""
      fi
      WORKER_COUNT=$((WORKER_COUNT + 1))
    done
  done

  echo "Fleet up: $WORKER_COUNT workers"
fi
WORKERS_JSON+="}"

# ---------------------------------------------------------------------------
# Get dev EBS volume ID (from the dev Terraform state)
# ---------------------------------------------------------------------------
echo ""
echo "Getting dev EBS volume ID..."
cd "$INFRA_DIR"
if [ ! -d ".terraform" ]; then
  echo "Error: dev infrastructure not initialized. Run 'just dev-setup' first." >&2
  exit 1
fi

DEV_VOLUME_ID=$(terraform output -raw ebs_volume_id 2>/dev/null || true)
if [ -z "$DEV_VOLUME_ID" ]; then
  echo "Error: could not read dev EBS volume ID. Is the dev instance provisioned?" >&2
  exit 1
fi
echo "Dev EBS volume: $DEV_VOLUME_ID"

# ---------------------------------------------------------------------------
# Create or reuse EBS snapshot
# ---------------------------------------------------------------------------
echo ""
echo "Looking for existing fleet snapshot..."
EXISTING_SNAP=$(aws ec2 describe-snapshots \
  --filters "Name=tag:Project,Values=rdp-fleet" "Name=status,Values=completed" \
  --query 'Snapshots | sort_by(@, &StartTime) | [-1].SnapshotId' \
  --output text 2>/dev/null || echo "None")

if [ "$EXISTING_SNAP" != "None" ] && [ -n "$EXISTING_SNAP" ]; then
  echo "Reusing existing snapshot: $EXISTING_SNAP"
  SNAPSHOT_ID="$EXISTING_SNAP"
else
  echo "Creating EBS snapshot from $DEV_VOLUME_ID..."
  SNAPSHOT_ID=$(aws ec2 create-snapshot \
    --volume-id "$DEV_VOLUME_ID" \
    --description "Fleet data volume snapshot" \
    --tag-specifications "ResourceType=snapshot,Tags=[{Key=Name,Value=rdp-fleet-data},{Key=Project,Value=rdp-fleet}]" \
    --query 'SnapshotId' --output text)
  echo "Snapshot: $SNAPSHOT_ID"
  echo "Waiting for snapshot to complete (this may take several minutes for large volumes)..."
  aws ec2 wait snapshot-completed --snapshot-ids "$SNAPSHOT_ID"
  echo "Snapshot ready."
fi

# ---------------------------------------------------------------------------
# Terraform apply
# ---------------------------------------------------------------------------
echo ""
echo "Applying Terraform configuration..."
cd "$FLEET_DIR"
if [ ! -d ".terraform" ]; then
  terraform init
fi

terraform apply -auto-approve \
  -var="snapshot_id=$SNAPSHOT_ID" \
  -var="workers=$WORKERS_JSON"

# ---------------------------------------------------------------------------
# Wait for instances + SSM, then install just
# ---------------------------------------------------------------------------
echo ""
echo "Waiting for instances to pass status checks..."
INSTANCES_JSON=$(terraform output -json worker_instances)
INSTANCE_IDS=$(python3 -c "import json,sys; d=json.load(sys.stdin); [print(v) for v in d.values()]" <<<"$INSTANCES_JSON")

for id in $INSTANCE_IDS; do
  aws ec2 wait instance-status-ok --instance-ids "$id" &
done
wait
echo "All instances running."

echo ""
echo "Waiting for SSM agent and installing just..."
PIDS=()
for id in $INSTANCE_IDS; do
  (
    for i in {1..60}; do
      STATUS=$(aws ssm describe-instance-information \
        --filters "Key=InstanceIds,Values=$id" \
        --query 'InstanceInformationList[0].PingStatus' \
        --output text 2>/dev/null || echo "")
      if [ "$STATUS" = "Online" ]; then break; fi
      sleep 5
    done

    INSTALL_CMD_ID=$(aws ssm send-command \
      --instance-ids "$id" \
      --document-name "AWS-RunShellScript" \
      --parameters '{"commands":["curl --proto =https --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin"]}' \
      --query 'Command.CommandId' --output text)

    for i in {1..30}; do
      RESULT=$(aws ssm get-command-invocation \
        --command-id "$INSTALL_CMD_ID" --instance-id "$id" \
        --query 'Status' --output text 2>/dev/null || echo "InProgress")
      case "$RESULT" in
      Success) break ;;
      Failed | TimedOut | Cancelled)
        echo "Warning: just install failed on $id (status: $RESULT)" >&2
        break
        ;;
      esac
      sleep 3
    done
  ) &
  PIDS+=($!)
done

for pid in "${PIDS[@]}"; do wait "$pid"; done
echo "Just installed on all workers."

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "Fleet ready!"
python3 -c "
import json, sys
d = json.load(sys.stdin)
for name, iid in sorted(d.items()):
    print(f'  {name}: {iid}')
" <<<"$INSTANCES_JSON"
