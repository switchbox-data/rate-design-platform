#!/usr/bin/env bash
# Resolve the latest timestamped CAIRO output directory for a given run.
#
# Usage:
#   latest_run_output.sh <scenario_config> <run_num>
#
# Reads run_name and path_outputs from the scenario YAML, converts
# parent(path_outputs) to an S3 URI, then uses `aws s3 ls` to find the
# most recent directory matching *_<run_name>/.
#
# Prints the full S3 URI (with trailing slash) to stdout.
# Exits non-zero with a message on stderr if no match is found.

set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <scenario_config> <run_num>" >&2
  exit 1
fi

scenario_config="$1"
run_num="$2"

# Extract run_name and path_outputs from YAML via Python (pyyaml).
read -r run_name path_outputs < <(
  python3 -c "
import sys, yaml
with open('$scenario_config') as f:
    data = yaml.safe_load(f)
runs = data.get('runs', {})
run = runs.get($run_num) or runs.get('$run_num')
if run is None:
    print(f'Run $run_num not found in $scenario_config', file=sys.stderr)
    sys.exit(1)
run_name = run.get('run_name', '')
path_outputs = run.get('path_outputs', '')
if not run_name:
    print(f'run_name missing for run $run_num', file=sys.stderr)
    sys.exit(1)
if not path_outputs:
    print(f'path_outputs missing for run $run_num', file=sys.stderr)
    sys.exit(1)
print(run_name, path_outputs)
"
)

# Derive parent directory and convert local /data.sb/ mount path to s3://data.sb/
parent_dir=$(dirname "$path_outputs")
s3_parent="${parent_dir/#\/data.sb\//s3://data.sb/}"

# Ensure trailing slash for S3 prefix listing
[[ "$s3_parent" == */ ]] || s3_parent="${s3_parent}/"

# List matching directories, sort lexicographically (timestamps sort naturally),
# take the last (most recent) one.
match=$(
  aws s3 ls "$s3_parent" 2>/dev/null \
    | grep -F "PRE" \
    | awk '{print $NF}' \
    | grep -F "${run_name}" \
    | sort \
    | tail -1
) || true

if [[ -z "$match" ]]; then
  echo "No output directory found matching run_name='${run_name}' under ${s3_parent}" >&2
  exit 1
fi

echo "${s3_parent}${match}"
