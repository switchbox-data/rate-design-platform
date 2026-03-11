#!/usr/bin/env bash
set -euo pipefail

# Sync code on fleet workers: git pull + uv sync.
#
# Usage:
#   fleet-sync.sh              # sync all workers
#   fleet-sync.sh all          # sync all workers
#   fleet-sync.sh ny-coned     # sync a specific worker by name
#
# Requires: GH_PAT env var (for initial clone if repo not yet present)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
FLEET_DIR="$SCRIPT_DIR"

# ---------------------------------------------------------------------------
# AWS credentials
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
# Parse target
# ---------------------------------------------------------------------------
TARGET="${1:-all}"

# ---------------------------------------------------------------------------
# Get worker instance IDs from Terraform
# ---------------------------------------------------------------------------
cd "$FLEET_DIR"
INSTANCES_JSON=$(terraform output -json worker_instances 2>/dev/null || echo "{}")

WORKER_NAMES=()
INSTANCE_IDS=()

if [ "$TARGET" = "all" ]; then
  while IFS= read -r line; do
    name=$(echo "$line" | cut -d' ' -f1)
    iid=$(echo "$line" | cut -d' ' -f2)
    WORKER_NAMES+=("$name")
    INSTANCE_IDS+=("$iid")
  done < <(python3 -c "
import json, sys
d = json.load(sys.stdin)
for k, v in sorted(d.items()):
    print(f'{k} {v}')
" <<<"$INSTANCES_JSON")
else
  iid=$(python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('$TARGET',''))" <<<"$INSTANCES_JSON")
  if [ -z "$iid" ]; then
    echo "Error: no fleet worker named '$TARGET'." >&2
    echo "Available workers:" >&2
    python3 -c "import json,sys; [print(f'  {k}') for k in sorted(json.load(sys.stdin))]" <<<"$INSTANCES_JSON" >&2
    exit 1
  fi
  WORKER_NAMES+=("$TARGET")
  INSTANCE_IDS+=("$iid")
fi

WORKER_COUNT=${#WORKER_NAMES[@]}
if [ "$WORKER_COUNT" -eq 0 ]; then
  echo "No fleet workers found. Run fleet-up first." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Determine git branch and repo URL
# ---------------------------------------------------------------------------
BRANCH=$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)

if [ -z "${GH_PAT:-}" ]; then
  echo "Warning: GH_PAT not set. Sync will fail if repo is not already cloned on workers." >&2
fi

echo "Syncing $WORKER_COUNT worker(s) to branch $BRANCH..."

# ---------------------------------------------------------------------------
# Send sync commands
# ---------------------------------------------------------------------------
CMD_IDS=()
for i in $(seq 0 $((WORKER_COUNT - 1))); do
  name="${WORKER_NAMES[$i]}"
  iid="${INSTANCE_IDS[$i]}"

  SYNC_SCRIPT="#!/bin/bash
set -euo pipefail
export HOME=/ebs/home/fleet
export PATH=\"/usr/local/bin:\$PATH\"
REPO_DIR=\"\$HOME/rate-design-platform\"

if [ ! -d \"\$REPO_DIR/.git\" ]; then
    echo \"Repo not found, cloning...\"
    mkdir -p \$HOME
    git clone --branch \"$BRANCH\" --single-branch \\
        \"https://x-access-token:${GH_PAT:-none}@github.com/switchbox-data/rate-design-platform.git\" \\
        \"\$REPO_DIR\"
else
    cd \"\$REPO_DIR\"
    git fetch origin
    git checkout \"$BRANCH\"
    git pull origin \"$BRANCH\"
fi

cd \"\$REPO_DIR\"
uv sync --python 3.13
echo \"Sync complete on \$(hostname)\"
"

  CMD_ID=$(python3 -c "
import json, subprocess, sys

script = sys.stdin.read()
cmd_input = {
    'InstanceIds': ['$iid'],
    'DocumentName': 'AWS-RunShellScript',
    'TimeoutSeconds': 600,
    'Parameters': {
        'commands': [
            'cat > /tmp/fleet-sync.sh << \"FLEET_SYNC_EOF\"',
            script.strip(),
            'FLEET_SYNC_EOF',
            'chmod +x /tmp/fleet-sync.sh',
            'bash /tmp/fleet-sync.sh'
        ]
    }
}

result = subprocess.run(
    ['aws', 'ssm', 'send-command',
     '--cli-input-json', json.dumps(cmd_input),
     '--query', 'Command.CommandId',
     '--output', 'text'],
    capture_output=True, text=True, check=True
)
print(result.stdout.strip())
" <<<"$SYNC_SCRIPT")

  CMD_IDS+=("$CMD_ID")
  echo "  $name → $iid (cmd: $CMD_ID)"
done

# ---------------------------------------------------------------------------
# Wait for completion
# ---------------------------------------------------------------------------
echo ""
echo "Waiting for sync to complete..."

FAILURES=0
for i in $(seq 0 $((WORKER_COUNT - 1))); do
  name="${WORKER_NAMES[$i]}"
  iid="${INSTANCE_IDS[$i]}"
  cmd_id="${CMD_IDS[$i]}"

  for attempt in {1..60}; do
    STATUS=$(aws ssm get-command-invocation \
      --command-id "$cmd_id" --instance-id "$iid" \
      --query 'Status' --output text 2>/dev/null || echo "InProgress")

    case "$STATUS" in
    Success)
      echo "  $name: synced"
      break
      ;;
    Failed | TimedOut | Cancelled)
      echo "  $name: FAILED ($STATUS)"
      OUTPUT=$(aws ssm get-command-invocation \
        --command-id "$cmd_id" --instance-id "$iid" \
        --query 'StandardErrorContent' --output text 2>/dev/null || echo "")
      if [ -n "$OUTPUT" ]; then
        echo "    stderr: $OUTPUT" | head -10
      fi
      FAILURES=$((FAILURES + 1))
      break
      ;;
    esac
    sleep 5
  done
done

echo ""
if [ "$FAILURES" -gt 0 ]; then
  echo "$FAILURES worker(s) failed to sync."
  exit 1
fi
echo "All $WORKER_COUNT worker(s) synced."
