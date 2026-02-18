#!/usr/bin/env python3
"""
Data Access Test Runner

This script runs all unit tests and integration tests for the data access layer.
It provides detailed reporting and can be used for continuous integration.
"""

import sys
import unittest
import os
from pathlib import Path

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Load .env file from project root
    project_root = Path(__file__).parent.parent.parent
    env_file = project_root / '.env'
    if env_file.exists():
        load_dotenv(env_file)
        print(f"✓ Loaded environment variables from {env_file}")
    else:
        print(f"⚠️  No .env file found at {env_file}")
except ImportError:
    print("⚠️  python-dotenv not installed. Install with: pip install python-dotenv")
    # Fallback: manually load .env file
    project_root = Path(__file__).parent.parent.parent
    env_file = project_root / '.env'
    if env_file.exists():
        print(f"✓ Manually loading environment variables from {env_file}")
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
    else:
        print(f"⚠️  No .env file found at {env_file}")

# Add the ripple module to Python path
sys.path.insert(0, str(Path(__file__).parent))

def discover_and_run_tests(test_directory: str = "ripple/tests/data_access",
                          pattern: str = "test_*.py",
                          verbosity: int = 2) -> unittest.TestResult:
    """
    Discover and run all tests in the specified directory.

    Args:
        test_directory: Directory containing test files
        pattern: Pattern for test file discovery
        verbosity: Test runner verbosity level

    Returns:
        TestResult object with test results
    """
    print("="*80)
    print("RIPPLe Data Access Layer - Test Suite")
    print("="*80)
    print(f"Test Directory: {test_directory}")
    print(f"Test Pattern: {pattern}")
    print(f"Verbosity Level: {verbosity}")
    print("="*80)

    # Discover tests
    loader = unittest.TestLoader()
    # Get project root by going up two levels from utils directory
    project_root = Path(__file__).parent.parent.parent
    start_dir = project_root / test_directory

    if not start_dir.exists():
        print(f"❌ Test directory not found: {start_dir}")
        return None

    suite = loader.discover(str(start_dir), pattern=pattern)

    # Run tests
    runner = unittest.TextTestRunner(verbosity=verbosity, buffer=True)
    result = runner.run(suite)

    return result


def print_test_summary(result: unittest.TestResult) -> None:
    """
    Print a detailed summary of test results.

    Args:
        result: TestResult object from test execution
    """
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)

    total_tests = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    skipped = len(result.skipped) if hasattr(result, 'skipped') else 0
    passed = total_tests - failures - errors - skipped

    print(f"Total Tests Run: {total_tests}")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failures}")
    print(f"🚫 Errors: {errors}")
    print(f"⏭️  Skipped: {skipped}")

    success_rate = (passed / total_tests * 100) if total_tests > 0 else 0
    print(f"Success Rate: {success_rate:.1f}%")

    # Print failure details if any
    if failures:
        print(f"\n{'='*80}")
        print("FAILURES:")
        print(f"{'='*80}")
        for test, traceback in result.failures:
            print(f"\n❌ {test}")
            print("-" * 60)
            print(traceback.split("AssertionError:")[-1].strip())

    # Print error details if any
    if errors:
        print(f"\n{'='*80}")
        print("ERRORS:")
        print(f"{'='*80}")
        for test, traceback in result.errors:
            print(f"\n🚫 {test}")
            print("-" * 60)
            print(traceback.split("Exception:")[-1].strip())

    # Print skipped tests if any
    if skipped:
        print(f"\n{'='*80}")
        print("SKIPPED:")
        print(f"{'='*80}")
        for test, reason in result.skipped:
            print(f"\n⏭️  {test}: {reason}")

    print("\n" + "="*80)

    # Overall result
    if result.wasSuccessful():
        print("🎉 ALL TESTS PASSED! 🎉")
        return 0
    else:
        print("💥 SOME TESTS FAILED!")
        return 1


def run_specific_test_module(module_name: str, verbosity: int = 2) -> unittest.TestResult:
    """
    Run tests for a specific module.

    Args:
        module_name: Name of the test module (e.g., 'test_service_monitor')
        verbosity: Test runner verbosity level

    Returns:
        TestResult object with test results
    """
    print(f"Running tests for module: {module_name}")

    # Import the module
    module_path = f"ripple.tests.data_access.{module_name}"

    try:
        suite = unittest.TestLoader().loadTestsFromName(module_path)
        runner = unittest.TextTestRunner(verbosity=verbosity, buffer=True)
        result = runner.run(suite)
        return result
    except ImportError as e:
        print(f"❌ Could not import module {module_path}: {e}")
        return None


def main():
    """Main function to run tests."""
    import argparse

    parser = argparse.ArgumentParser(description="Run RIPPLe Data Access Layer Tests")
    parser.add_argument("--module", "-m", type=str,
                       help="Run tests for a specific module (e.g., test_service_monitor)")
    parser.add_argument("--directory", "-d", type=str, default="ripple/tests/data_access",
                       help="Test directory path")
    parser.add_argument("--pattern", "-p", type=str, default="test_*.py",
                       help="Test file pattern")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Increase verbosity")
    parser.add_argument("--quiet", "-q", action="store_true",
                       help="Decrease verbosity")

    args = parser.parse_args()

    # Set verbosity level
    if args.verbose:
        verbosity = 2
    elif args.quiet:
        verbosity = 0
    else:
        verbosity = 1

    # Check environment setup
    print("Checking environment...")

    # Check if we're in the right directory
    if not Path("ripple").exists():
        print("❌ Error: Not in RIPPLe project directory. Please run from project root.")
        return 1

    # Set up test environment if needed
    os.environ['PYTHONPATH'] = str(Path(__file__).parent)

    # Check for test token
    test_token = os.environ.get("RSP_ACCESS_TOKEN")
    if test_token:
        print(f"✓ Found RSP access token: {test_token[:10]}...")
    else:
        print("⚠️  No RSP_ACCESS_TOKEN found - some tests may be skipped")
        print("   Set environment variable for full test coverage")

    print()

    # Run tests
    if args.module:
        # Run specific module
        result = run_specific_test_module(args.module, verbosity)
    else:
        # Run all tests
        result = discover_and_run_tests(args.directory, args.pattern, verbosity)

    if result is None:
        return 1

    # Print summary and return exit code
    exit_code = print_test_summary(result)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())