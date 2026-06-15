"""Import-safe helpers for testing logging from imported modules."""

import logging
import sys

_module_logger = logging.getLogger("imported_logging_demo")


def log_from_imported_module() -> None:
    """Emit output via print, raw streams, and stdlib logging."""
    print("[IMPORTED] print from imported module")
    sys.stdout.write("[IMPORTED] raw stdout.write from imported module\n")
    sys.stderr.write("[IMPORTED] raw stderr.write from imported module\n")
    _module_logger.info("[IMPORTED] stdlib logging.info from imported module")


def log_with_prefect_logger() -> None:
    """Call get_run_logger at runtime (safe when invoked from a task)."""
    from prefect.logging import get_run_logger

    logger = get_run_logger()
    logger.info("[IMPORTED] get_run_logger from imported module")
