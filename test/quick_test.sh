#!/bin/bash
# Quick test runner script for CIFFile test suite
# Usage: ./quick_test.sh [option]

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}CIFFile Test Suite${NC}"
echo "===================="

case "$1" in
  "all"|"")
    echo -e "${GREEN}Running all tests...${NC}"
    pytest -v
    ;;
  "fast")
    echo -e "${GREEN}Running fast tests only...${NC}"
    pytest -v -m "not slow"
    ;;
  "unit")
    echo -e "${GREEN}Running unit tests...${NC}"
    pytest -v -m unit
    ;;
  "integration")
    echo -e "${GREEN}Running integration tests...${NC}"
    pytest -v -m integration
    ;;
  "coverage")
    echo -e "${GREEN}Running tests with coverage...${NC}"
    pytest -v --cov=ciffile --cov-report=html --cov-report=term
    echo -e "${GREEN}Coverage report generated in htmlcov/index.html${NC}"
    ;;
  "parallel")
    echo -e "${GREEN}Running tests in parallel...${NC}"
    pytest -v -n auto
    ;;
  "parser")
    echo -e "${GREEN}Running parser tests...${NC}"
    pytest -v -m parser
    ;;
  "reader")
    echo -e "${GREEN}Running reader tests...${NC}"
    pytest -v test_reader.py
    ;;
  "writer")
    echo -e "${GREEN}Running writer tests...${NC}"
    pytest -v test_writer.py
    ;;
  "structure")
    echo -e "${GREEN}Running structure tests...${NC}"
    pytest -v -m structure
    ;;
  "help")
    echo "Usage: ./quick_test.sh [option]"
    echo ""
    echo "Options:"
    echo "  all          - Run all tests (default)"
    echo "  fast         - Run only fast tests (skip slow tests)"
    echo "  unit         - Run only unit tests"
    echo "  integration  - Run only integration tests"
    echo "  coverage     - Run with coverage report"
    echo "  parallel     - Run tests in parallel"
    echo "  parser       - Run parser tests only"
    echo "  reader       - Run reader tests only"
    echo "  writer       - Run writer tests only"
    echo "  structure    - Run structure tests only"
    echo "  help         - Show this help message"
    ;;
  *)
    echo "Unknown option: $1"
    echo "Use './quick_test.sh help' for usage information"
    exit 1
    ;;
esac
