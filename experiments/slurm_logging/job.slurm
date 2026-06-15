#!/bin/bash
#SBATCH --job-name=prefect-log-test
#SBATCH --time=00:02:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --partition=t2small
#
# Log paths and --chdir are set on the sbatch command line so files land in the
# user's experiment directory (e.g. ~/prefect_slurm_logging_experiment/logs/),
# not under /var/spool/slurm/ where the job has no write permission.

set -euo pipefail

export PATH="${HOME}/.local/bin:${HOME}/.cargo/bin:${PATH}"

WORKDIR="${SLURM_SUBMIT_DIR:?SLURM_SUBMIT_DIR is not set}"
FAIL_MODE="${1:-none}"

echo "[SBATCH] wrapper stdout at $(date)"
echo "[SBATCH] wrapper stderr at $(date)" >&2
echo "[SBATCH] workdir=${WORKDIR} fail_mode=${FAIL_MODE}"

WORKER="${WORKDIR}/worker.py"

echo "[SBATCH] launching worker with --fail=${FAIL_MODE}"
uv run python "${WORKER}" --fail "${FAIL_MODE}" --sleep 3
EXIT=$?
echo "[SBATCH] worker exit code: ${EXIT}"
exit "${EXIT}"
