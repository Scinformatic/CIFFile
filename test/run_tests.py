"""Main test runner script for the CIFFile test suite.

This script provides a convenient way to run the complete test suite
with various options and configurations.

Usage:
    python run_tests.py [options]

Examples:
    # Run all tests
    python run_tests.py

    # Run with coverage report
    python run_tests.py --coverage

    # Run only unit tests
    python run_tests.py --unit

    # Run only fast tests
    python run_tests.py --fast

    # Run in parallel
    python run_tests.py --parallel
"""

import sys
import subprocess
from pathlib import Path
from typing import List


def run_pytest(args: List[str]) -> int:
    """Run pytest with the given arguments.

    Parameters
    ----------
    args : List[str]
        Command-line arguments to pass to pytest.

    Returns
    -------
    int
        Exit code from pytest.
    """
    cmd = ["pytest"] + args
    print(f"Running: {' '.join(cmd)}")
    return subprocess.call(cmd)


def main() -> int:
    """Main entry point for test runner.

    Returns
    -------
    int
        Exit code (0 for success, non-zero for failure).
    """
    # Parse command-line arguments
    import argparse

    parser = argparse.ArgumentParser(
        description="Run CIFFile test suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )

    parser.add_argument(
        "-c", "--coverage",
        action="store_true",
        help="Run with coverage report",
    )

    parser.add_argument(
        "-u", "--unit",
        action="store_true",
        help="Run only unit tests",
    )

    parser.add_argument(
        "-i", "--integration",
        action="store_true",
        help="Run only integration tests",
    )

    parser.add_argument(
        "-f", "--fast",
        action="store_true",
        help="Run only fast tests (skip slow tests)",
    )

    parser.add_argument(
        "-p", "--parallel",
        action="store_true",
        help="Run tests in parallel",
    )

    parser.add_argument(
        "-m", "--marker",
        type=str,
        help="Run tests with specific marker",
    )

    parser.add_argument(
        "-k", "--keyword",
        type=str,
        help="Run tests matching keyword expression",
    )

    parser.add_argument(
        "tests",
        nargs="*",
        help="Specific test files or patterns to run",
    )

    args = parser.parse_args()

    # Build pytest arguments
    pytest_args = []

    # Verbosity
    if args.verbose:
        pytest_args.append("-v")

    # Coverage
    if args.coverage:
        pytest_args.extend([
            "--cov=ciffile",
            "--cov-report=html",
            "--cov-report=term",
        ])

    # Markers
    if args.unit:
        pytest_args.extend(["-m", "unit"])
    elif args.integration:
        pytest_args.extend(["-m", "integration"])
    elif args.marker:
        pytest_args.extend(["-m", args.marker])

    # Fast mode
    if args.fast:
        pytest_args.extend(["-m", "not slow"])

    # Parallel execution
    if args.parallel:
        pytest_args.extend(["-n", "auto"])

    # Keyword filter
    if args.keyword:
        pytest_args.extend(["-k", args.keyword])

    # Specific tests
    if args.tests:
        pytest_args.extend(args.tests)

    # Run pytest
    return run_pytest(pytest_args)


if __name__ == "__main__":
    sys.exit(main())
