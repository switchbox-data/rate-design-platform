#!/usr/bin/env bash
# Resolve the latest CAIRO output directory for a given run.
#
# Usage:
#   latest_run_output.sh <scenario_config> <run_num>
#
# Reads run_name and path_outputs from the scenario YAML. path_outputs contains
# an <execution_time> placeholder, e.g.:
#   /data.sb/.../ny/coned/<execution_time>/ny_coned_run1_up00_precalc__flat
#
# The script goes two levels up from path_outputs to get the utility base dir
# (past <execution_time>/ and the run name), converts it to an S3 URI, finds the
# latest execution_time directory, and searches ONLY within it for *_<run_name>/.
#
# Prints the full S3 URI (no trailing slash) so callers can safely append /filename.
# Exits non-zero with a helpful message on stderr if no match is found.

set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <scenario_config> <run_num>" >&2
  exit 1
fi

scenario_config="$1"
run_num="$2"

# Extract run_name and utility base dir from YAML via Python (pyyaml).
# path_outputs contains <execution_time>, so we go two levels up to get the
# utility-level directory (past <execution_time>/ and the run name basename).
read -r run_name base_dir < <(
  uv run python3 -c "
import sys, yaml, os
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
base_dir = os.path.dirname(os.path.dirname(path_outputs))
print(run_name, base_dir)
"
)

# Convert local /data.sb/ mount path to s3://data.sb/
s3_parent="${base_dir/#\/data.sb\//s3://data.sb/}"

# Ensure trailing slash for S3 prefix listing
[[ "$s3_parent" == */ ]] || s3_parent="${s3_parent}/"

# Find the latest execution_time directory
latest_et=$(
  aws s3 ls "$s3_parent" 2>/dev/null |
    grep -F "PRE" |
    awk '{print $NF}' |
    sort |
    tail -1
) || true

if [[ -z "$latest_et" ]]; then
  echo "No execution-time directories found under ${s3_parent}" >&2
  exit 1
fi

# Search ONLY within the latest execution_time dir for *_<run_name>/
match=$(
  aws s3 ls "${s3_parent}${latest_et}" 2>/dev/null |
    grep -F "PRE" |
    awk '{print $NF}' |
    grep -F "${run_name}" |
    sort |
    tail -1
) || true

if [[ -z "$match" ]]; then
  echo "Run '${run_name}' not found in latest batch ${latest_et%/}" >&2
  echo "  Searched: ${s3_parent}${latest_et}" >&2
  echo "  Re-run it in the current batch, or run a full batch." >&2
  exit 1
fi

# Strip trailing slash so "${run_dir}/filename" does not produce double slash
result="${s3_parent}${latest_et}${match}"
echo "${result%/}"
