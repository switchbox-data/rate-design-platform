#!/usr/bin/env bash
set -euo pipefail

# Dispatch CAIRO runs to fleet workers and monitor progress.
#
# Usage:
#   fleet-dispatch.sh <state1> [state2 ...]       # all utilities for each state
#   fleet-dispatch.sh <state>:<utility> [...]      # specific utilities only
#   fleet-dispatch.sh --test                       # smoke test (trivial command)
#
# Requires:
#   - Fleet workers up (fleet-up.sh)
#   - GH_PAT env var (for cloning the repo on workers)

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
# Parse arguments
# ---------------------------------------------------------------------------
TEST_MODE=false
ARGS=()
for arg in "$@"; do
  case "$arg" in
  --test) TEST_MODE=true ;;
  *) ARGS+=("$arg") ;;
  esac
done

# ---------------------------------------------------------------------------
# Build work items: parallel arrays of (worker_name, state, utility)
# ---------------------------------------------------------------------------
WORK_NAMES=()
WORK_STATES=()
WORK_UTILS=()

if [ "$TEST_MODE" = true ]; then
  WORK_NAMES+=("test-worker-0" "test-worker-1")
  WORK_STATES+=("test" "test")
  WORK_UTILS+=("smoke-0" "smoke-1")
else
  if [ ${#ARGS[@]} -eq 0 ]; then
    echo "Usage: fleet-dispatch.sh [--test] <state1> [state2 ...] or <state>:<utility> [...]" >&2
    exit 1
  fi

  HP_RATES_DIR="$REPO_ROOT/rate_design/hp_rates"
  for arg in "${ARGS[@]}"; do
    if [[ "$arg" == *":"* ]]; then
      state="${arg%%:*}"
      util="${arg#*:}"
      WORK_NAMES+=("${state}-${util}")
      WORK_STATES+=("$state")
      WORK_UTILS+=("$util")
    else
      state="$arg"
      STATE_ENV="$HP_RATES_DIR/$state/state.env"
      if [ ! -f "$STATE_ENV" ]; then
        echo "Error: state.env not found at $STATE_ENV" >&2
        exit 1
      fi
      UTILITIES=$(grep '^UTILITIES=' "$STATE_ENV" | cut -d= -f2)
      IFS=',' read -ra utils <<<"$UTILITIES"
      for util in "${utils[@]}"; do
        WORK_NAMES+=("${state}-${util}")
        WORK_STATES+=("$state")
        WORK_UTILS+=("$util")
      done
    fi
  done

  if [ -z "${GH_PAT:-}" ]; then
    echo "Error: GH_PAT environment variable is required (for cloning the repo on workers)." >&2
    exit 1
  fi
fi

WORK_COUNT=${#WORK_NAMES[@]}

# ---------------------------------------------------------------------------
# Get worker instance IDs from Terraform
# ---------------------------------------------------------------------------
cd "$FLEET_DIR"
INSTANCES_JSON=$(terraform output -json worker_instances 2>/dev/null || echo "{}")

INSTANCE_IDS=()
for name in "${WORK_NAMES[@]}"; do
  id=$(python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('$name',''))" <<<"$INSTANCES_JSON")
  if [ -z "$id" ]; then
    echo "Error: no fleet worker named '$name'. Did you run fleet-up with the right states?" >&2
    exit 1
  fi
  INSTANCE_IDS+=("$id")
done

# ---------------------------------------------------------------------------
# Prepare execution context
# ---------------------------------------------------------------------------
if [ -z "${RDP_BATCH:-}" ]; then
  echo "Error: RDP_BATCH environment variable is required." >&2
  echo "  Example: RDP_BATCH=ny_20260305c_r1-8 ./fleet-dispatch.sh ny" >&2
  exit 1
fi

BRANCH=$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)
LOG_DIR="$REPO_ROOT/logs/fleet-$RDP_BATCH"
mkdir -p "$LOG_DIR"

echo "Fleet dispatch: $WORK_COUNT workers, branch=$BRANCH, batch=$RDP_BATCH"
echo "Logs: $LOG_DIR"
echo ""

# ---------------------------------------------------------------------------
# Send SSM commands
# ---------------------------------------------------------------------------
COMMAND_IDS=()
STATUSES=()
START_EPOCH=$(date +%s)

for i in $(seq 0 $((WORK_COUNT - 1))); do
  name="${WORK_NAMES[$i]}"
  state="${WORK_STATES[$i]}"
  util="${WORK_UTILS[$i]}"
  instance_id="${INSTANCE_IDS[$i]}"

  if [ "$TEST_MODE" = true ]; then
    WORKER_SCRIPT="#!/bin/bash
set -euo pipefail
echo \"Worker \$(hostname) ready\"
echo \"just: \$(just --version 2>&1 || echo 'not found')\"
echo \"uv: \$(uv --version 2>&1 || echo 'not found')\"
echo \"python3: \$(python3 --version 2>&1 || echo 'not found')\"
echo \"EBS mount:\"
df -h /ebs 2>/dev/null || echo '/ebs not mounted'
echo \"S3 mount:\"
ls /data.sb/ 2>/dev/null | head -5 || echo '/data.sb not mounted'
echo \"Test complete for $name\"
"
  else
    WORKER_SCRIPT="#!/bin/bash
set -euo pipefail
export HOME=/ebs/home/fleet
export PATH=\"/usr/local/bin:\$PATH\"
export TMPDIR=/ebs/tmp
export AWS_DEFAULT_REGION=us-west-2
mkdir -p \$HOME

REPO_DIR=\"\$HOME/rate-design-platform\"
if [ ! -d \"\$REPO_DIR/.git\" ]; then
    echo \"Cloning repo (branch: $BRANCH)...\"
    git clone --branch \"$BRANCH\" --single-branch \\
        \"https://x-access-token:${GH_PAT}@github.com/switchbox-data/rate-design-platform.git\" \\
        \"\$REPO_DIR\"
else
    echo \"Repo exists, checking out $BRANCH...\"
    cd \"\$REPO_DIR\"
    git fetch origin
    git checkout \"$BRANCH\"
    git pull origin \"$BRANCH\"
fi

cd \"\$REPO_DIR\"
echo \"Installing dependencies...\"
uv sync --python 3.13

echo \"Running all-pre for $state/$util...\"
cd rate_design/hp_rates
export RDP_BATCH=\"$RDP_BATCH\"
UTILITY=\"$util\" just s \"$state\" all-pre

echo \"Running all-sequential for $state/$util...\"
UTILITY=\"$util\" just s \"$state\" run-all-sequential

echo \"Done: $state/$util\"
"
  fi

  # Use python to safely JSON-encode the script and build the SSM command
  CMD_ID=$(python3 -c "
import json, subprocess, sys

script = sys.stdin.read()
cmd_input = {
    'InstanceIds': ['$instance_id'],
    'DocumentName': 'AWS-RunShellScript',
    'TimeoutSeconds': 28800,
    'OutputS3BucketName': 'data.sb',
    'OutputS3KeyPrefix': 'switchbox/fleet-logs/$RDP_BATCH',
    'Parameters': {
        'commands': [
            'cat > /tmp/fleet-run.sh << \"FLEET_SCRIPT_EOF\"',
            script.strip(),
            'FLEET_SCRIPT_EOF',
            'chmod +x /tmp/fleet-run.sh',
            'bash /tmp/fleet-run.sh'
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
" <<<"$WORKER_SCRIPT")

  COMMAND_IDS+=("$CMD_ID")
  STATUSES+=("InProgress")
  echo "  Dispatched $name → $instance_id (cmd: $CMD_ID)"
done

echo ""
echo "All commands dispatched. Monitoring progress..."
echo ""

# ---------------------------------------------------------------------------
# Poll-and-display loop
# ---------------------------------------------------------------------------
TABLE_LINES=0

print_status_table() {
  # Overwrite previous table
  if [ "$TABLE_LINES" -gt 0 ]; then
    printf "\033[%dA\033[J" "$TABLE_LINES"
  fi

  local now
  now=$(date +%s)
  local in_progress=0 succeeded=0 failed=0

  printf "Fleet %s — %s\n" "$RDP_BATCH" "$(date +%H:%M:%S)"
  printf "%-20s %-20s %-12s %8s  %s\n" "WORKER" "INSTANCE" "STATUS" "ELAPSED" ""
  printf "%.0s─" {1..70}
  echo ""
  TABLE_LINES=3

  for i in $(seq 0 $((WORK_COUNT - 1))); do
    local name="${WORK_NAMES[$i]}"
    local iid="${INSTANCE_IDS[$i]}"
    local status="${STATUSES[$i]}"
    local elapsed=$(((now - START_EPOCH) / 60))
    local marker=""

    case "$status" in
    Success)
      marker="done"
      succeeded=$((succeeded + 1))
      ;;
    Failed | TimedOut | Cancelled)
      marker="FAILED"
      failed=$((failed + 1))
      ;;
    *) in_progress=$((in_progress + 1)) ;;
    esac

    printf "  %-18s %-20s %-12s %6dm  %s\n" "$name" "$iid" "$status" "$elapsed" "$marker"
    TABLE_LINES=$((TABLE_LINES + 1))
  done

  printf "%.0s─" {1..70}
  echo ""
  printf "  %d in progress · %d done · %d failed\n" "$in_progress" "$succeeded" "$failed"
  TABLE_LINES=$((TABLE_LINES + 2))
}

download_log() {
  local idx="$1"
  local name="${WORK_NAMES[$idx]}"
  local cmd_id="${COMMAND_IDS[$idx]}"
  local iid="${INSTANCE_IDS[$idx]}"
  local log_file="$LOG_DIR/${name}.log"

  # SSM S3 output path: <prefix>/<cmd_id>/<instance_id>/awsrunShellScript/0.awsrunShellScript/stdout
  local s3_stdout="s3://data.sb/switchbox/fleet-logs/$RDP_BATCH/$cmd_id/$iid/awsrunShellScript/0.awsrunShellScript/stdout"
  local s3_stderr="s3://data.sb/switchbox/fleet-logs/$RDP_BATCH/$cmd_id/$iid/awsrunShellScript/0.awsrunShellScript/stderr"

  {
    echo "=== STDOUT ==="
    aws s3 cp "$s3_stdout" - 2>/dev/null || echo "(no stdout in S3)"
    echo ""
    echo "=== STDERR ==="
    aws s3 cp "$s3_stderr" - 2>/dev/null || echo "(no stderr in S3)"
  } >"$log_file" 2>/dev/null

  # Also try SSM direct output as fallback (truncated at 24KB but useful for short runs)
  if [ ! -s "$log_file" ] || grep -q "^(no stdout in S3)$" "$log_file" 2>/dev/null; then
    local ssm_output
    ssm_output=$(aws ssm get-command-invocation \
      --command-id "$cmd_id" --instance-id "$iid" \
      --query '[StandardOutputContent, StandardErrorContent]' \
      --output text 2>/dev/null || echo "")
    if [ -n "$ssm_output" ]; then
      echo "$ssm_output" >"$log_file"
    fi
  fi
}

# Completed tracker (avoid re-downloading logs)
COMPLETED=()
for i in $(seq 0 $((WORK_COUNT - 1))); do COMPLETED+=(false); done

while true; do
  ALL_DONE=true
  for i in $(seq 0 $((WORK_COUNT - 1))); do
    if [ "${COMPLETED[$i]}" = "true" ]; then continue; fi

    status=$(aws ssm get-command-invocation \
      --command-id "${COMMAND_IDS[$i]}" --instance-id "${INSTANCE_IDS[$i]}" \
      --query 'Status' --output text 2>/dev/null || echo "InProgress")
    STATUSES[$i]="$status"

    case "$status" in
    Success)
      COMPLETED[$i]=true
      download_log "$i"
      ;;
    Failed | TimedOut | Cancelled)
      COMPLETED[$i]=true
      download_log "$i"
      # Print failure tail after the status table
      ;;
    *)
      ALL_DONE=false
      ;;
    esac
  done

  print_status_table

  # Print failure details for newly completed failures
  for i in $(seq 0 $((WORK_COUNT - 1))); do
    if [ "${STATUSES[$i]}" = "Failed" ] || [ "${STATUSES[$i]}" = "TimedOut" ] || [ "${STATUSES[$i]}" = "Cancelled" ]; then
      local_log="$LOG_DIR/${WORK_NAMES[$i]}.log"
      if [ -f "$local_log" ]; then
        # Only print once: check if we already printed by looking for a marker file
        marker="$LOG_DIR/.printed-${WORK_NAMES[$i]}"
        if [ ! -f "$marker" ]; then
          echo ""
          echo "=== FAILURE: ${WORK_NAMES[$i]} (last 30 lines) ==="
          tail -30 "$local_log" 2>/dev/null || true
          echo "=== Full log: $local_log ==="
          echo ""
          touch "$marker"
          # Re-count table lines so next redraw starts from the right place
          TABLE_LINES=0
        fi
      fi
    fi
  done

  if [ "$ALL_DONE" = true ]; then break; fi
  sleep 30
done

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------
echo ""
echo "Fleet dispatch complete. Logs: $LOG_DIR"

FAIL_COUNT=0
for i in $(seq 0 $((WORK_COUNT - 1))); do
  case "${STATUSES[$i]}" in
  Failed | TimedOut | Cancelled) FAIL_COUNT=$((FAIL_COUNT + 1)) ;;
  esac
done

if [ "$FAIL_COUNT" -gt 0 ]; then
  echo "$FAIL_COUNT worker(s) failed."
  exit 1
fi

echo "All $WORK_COUNT workers succeeded."
