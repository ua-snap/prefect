#!/usr/bin/env python3
"""Dummy SLURM worker for Prefect logging experiments.

Exercises print, raw stdout/stderr, and stdlib logging so you can compare
what reaches Prefect UI vs SLURM .out/.err files on the cluster.
"""

import argparse
import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("dummy_python_worker_script")


def main() -> None:
    parser = argparse.ArgumentParser(description="Dummy SLURM worker for logging tests")
    parser.add_argument(
        "--fail",
        choices=["none", "exit", "timeout", "exception"],
        default="none",
        help="Failure mode to exercise (timeout requires a short SLURM time limit)",
    )
    parser.add_argument(
        "--sleep",
        type=int,
        default=2,
        help="Seconds to sleep before finishing or failing",
    )
    args = parser.parse_args()

    print("[WORKER] print to stdout")
    sys.stdout.write("[WORKER] raw stdout.write\n")
    sys.stdout.flush()

    print("[WORKER] print to stderr", file=sys.stderr)
    sys.stderr.write("[WORKER] raw stderr.write\n")
    sys.stderr.flush()

    logger.info("[WORKER] logging.info (default handler stderr)")
    logger.warning("[WORKER] logging.warning")

    time.sleep(args.sleep)

    if args.fail == "exit":
        logger.error("[WORKER] exiting with code 42")
        sys.exit(42)
    if args.fail == "exception":
        raise RuntimeError("[WORKER] intentional exception")
    if args.fail == "timeout":
        logger.warning("[WORKER] sleeping until SLURM kills the job")
        time.sleep(9999)

    logger.info("[WORKER] completed successfully")


if __name__ == "__main__":
    main()
