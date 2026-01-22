#!/usr/bin/env python3
"""
Milvus CLI Test Runner with Markdown Report Generation
Usage: python run_tests.py [--uri MILVUS_URI]
"""

import os
import sys
import argparse
import subprocess
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET


class Colors:
    """Terminal colors for output."""
    GREEN = '\033[0;32m'
    BLUE = '\033[0;34m'
    YELLOW = '\033[1;33m'
    RED = '\033[0;31m'
    BOLD = '\033[1m'
    NC = '\033[0m'


def print_header(text, color=Colors.BLUE):
    """Print a formatted header."""
    print(f"\n{color}{'=' * 60}{Colors.NC}")
    print(f"{color}{text}{Colors.NC}")
    print(f"{color}{'=' * 60}{Colors.NC}\n")


def print_info(text, icon="✓", color=Colors.GREEN):
    """Print an info message."""
    print(f"{color}{icon} {text}{Colors.NC}")


def run_tests(milvus_uri):
    """Run pytest and generate markdown report."""

    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    junit_xml = Path(f"test_results_{timestamp}.xml")
    md_report = Path(f"TEST_REPORT_{timestamp}.md")

    print_header("Milvus CLI Test Suite")
    print_info(f"Milvus URI: {milvus_uri}", "🔗", Colors.YELLOW)
    print_info(f"Timestamp: {timestamp}", "🕐", Colors.YELLOW)

    # Check virtual environment
    if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print_info("Warning: Not running in a virtual environment", "⚠️", Colors.YELLOW)

    print_header("Running Tests", Colors.GREEN)

    # Build pytest command
    pytest_cmd = [
        sys.executable, "-m", "pytest",
        "tests/",
        "-v",
        "--tb=short",
        f"--junit-xml={junit_xml}",
    ]

    # Set environment variable
    env = os.environ.copy()
    env["MILVUS_URI"] = milvus_uri

    # Run tests
    try:
        result = subprocess.run(
            pytest_cmd,
            env=env,
            capture_output=False,
            text=True
        )
        exit_code = result.returncode

    except FileNotFoundError:
        print_info("pytest not found. Installing...", "⚠️", Colors.YELLOW)
        subprocess.run([sys.executable, "-m", "pip", "install", "pytest", "-q"])
        return run_tests(milvus_uri)

    # Generate markdown report
    print_header("Generating Report")
    generate_markdown_report(junit_xml, md_report, milvus_uri)

    # Print final status
    print()
    if exit_code == 0:
        print_header("✓ ALL TESTS PASSED", Colors.GREEN)
    else:
        print_header("✗ SOME TESTS FAILED", Colors.RED)

    print_info(f"Test Report: {md_report}", "📄", Colors.YELLOW)

    # Clean up XML file
    if junit_xml.exists():
        junit_xml.unlink()

    return exit_code


def generate_markdown_report(junit_xml, md_report, milvus_uri):
    """Generate a markdown test report from JUnit XML."""

    try:
        # Parse JUnit XML
        tree = ET.parse(junit_xml)
        root = tree.getroot()

        # Get test suites
        testsuites = root if root.tag == 'testsuites' else [root]

        total = 0
        passed = 0
        failed = 0
        skipped = 0
        duration = 0.0
        failed_tests = []

        for testsuite in testsuites if root.tag == 'testsuites' else [root]:
            total += int(testsuite.get('tests', 0))
            failed += int(testsuite.get('failures', 0)) + int(testsuite.get('errors', 0))
            skipped += int(testsuite.get('skipped', 0))
            duration += float(testsuite.get('time', 0))

            # Collect failed test details
            for testcase in testsuite.findall('testcase'):
                failure = testcase.find('failure')
                error = testcase.find('error')
                if failure is not None or error is not None:
                    failed_tests.append({
                        'class': testcase.get('classname', ''),
                        'name': testcase.get('name', ''),
                        'message': (failure.get('message', '') if failure is not None
                                   else error.get('message', ''))
                    })

        passed = total - failed - skipped

        # Generate markdown
        with open(md_report, 'w') as f:
            f.write(f"# Milvus CLI Test Report\n\n")
            f.write(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Milvus URI**: {milvus_uri}\n")
            f.write(f"**Framework**: pytest\n\n")

            # Summary
            f.write(f"## Test Execution Summary\n\n")
            f.write(f"| Metric | Count |\n")
            f.write(f"|--------|-------|\n")
            f.write(f"| Total Tests | {total} |\n")
            f.write(f"| Passed | {passed} ✓ |\n")
            f.write(f"| Failed | {failed} ✗ |\n")
            f.write(f"| Skipped | {skipped} ⊘ |\n")
            f.write(f"| Duration | {duration:.2f}s |\n\n")

            # Overall status
            if failed == 0:
                f.write(f"**Overall Status**: ✅ ALL TESTS PASSED\n\n")
            else:
                f.write(f"**Overall Status**: ❌ {failed} TEST(S) FAILED\n\n")

            # Failed tests details
            if failed_tests:
                f.write(f"## Failed Tests\n\n")
                for test in failed_tests:
                    f.write(f"### ❌ {test['class']}::{test['name']}\n\n")
                    f.write(f"```\n{test['message']}\n```\n\n")

            # Test details by category
            f.write(f"## Test Details by Category\n\n")

            # Group tests by file
            tests_by_file = {}
            for testsuite in testsuites if root.tag == 'testsuites' else [root]:
                for testcase in testsuite.findall('testcase'):
                    classname = testcase.get('classname', '')
                    file_name = classname.split('.')[1] if '.' in classname else 'unknown'

                    if file_name not in tests_by_file:
                        tests_by_file[file_name] = {
                            'passed': 0,
                            'failed': 0,
                            'skipped': 0,
                            'tests': []
                        }

                    name = testcase.get('name', '')
                    time = float(testcase.get('time', 0))

                    # Check outcome
                    if testcase.find('failure') is not None or testcase.find('error') is not None:
                        icon = '❌'
                        outcome = 'FAILED'
                        tests_by_file[file_name]['failed'] += 1
                    elif testcase.find('skipped') is not None:
                        icon = '⊘'
                        outcome = 'SKIPPED'
                        tests_by_file[file_name]['skipped'] += 1
                    else:
                        icon = '✅'
                        outcome = 'PASSED'
                        tests_by_file[file_name]['passed'] += 1

                    tests_by_file[file_name]['tests'].append({
                        'icon': icon,
                        'name': name,
                        'time': time,
                        'outcome': outcome
                    })

            # Write test details
            for file_name, data in sorted(tests_by_file.items()):
                total_file = data['passed'] + data['failed'] + data['skipped']
                f.write(f"### {file_name} ({data['passed']}/{total_file} passed)\n\n")

                for test in data['tests']:
                    f.write(f"- {test['icon']} `{test['name']}` ({test['time']:.2f}s)\n")

                f.write(f"\n")

            # Footer
            f.write(f"---\n\n")
            f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        print_info(f"Report generated: {md_report}")

    except Exception as e:
        print_info(f"Could not generate report: {e}", "⚠️", Colors.YELLOW)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run Milvus CLI tests with markdown report generation"
    )
    parser.add_argument(
        "--uri",
        default=os.getenv("MILVUS_URI", "http://localhost:19530"),
        help="Milvus server URI (default: $MILVUS_URI or http://localhost:19530)"
    )

    args = parser.parse_args()

    exit_code = run_tests(args.uri)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
