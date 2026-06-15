#!/bin/bash
#SBATCH --job-name=prefect-log-test
#SBATCH --output=logs/slurm-%j.out
#SBATCH --error=logs/slurm-%j.err
#SBATCH --time=00:02:00
#SBATCH --ntasks=1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAIL_MODE="${1:-none}"

echo "[SBATCH] wrapper stdout at $(date)"
echo "[SBATCH] wrapper stderr at $(date)" >&2
echo "[SBATCH] script_dir=${SCRIPT_DIR} fail_mode=${FAIL_MODE}"

WORKER="${SCRIPT_DIR}/worker.py"
mkdir -p "${SCRIPT_DIR}/logs"

echo "[SBATCH] launching worker with --fail=${FAIL_MODE}"
python "${WORKER}" --fail "${FAIL_MODE}" --sleep 3
EXIT=$?
echo "[SBATCH] worker exit code: ${EXIT}"
exit "${EXIT}"
