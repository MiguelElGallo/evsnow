#!/bin/bash
#
# Verification script for orchestrator tests
# Run this after installing all dependencies to validate the test suite
#

set -e  # Exit on error

echo "==================================="
echo "Orchestrator Test Verification"
echo "==================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    echo -e "${RED}Error: Must be run from project root${NC}"
    exit 1
fi

# Check Python version
echo "1. Checking Python version..."
python_version=$(python --version 2>&1 | awk '{print $2}')
echo "   Python version: $python_version"

# Check if pytest is installed
echo ""
echo "2. Checking pytest installation..."
if ! command -v pytest &> /dev/null; then
    echo -e "${RED}   ✗ pytest not found${NC}"
    echo "   Install with: pip install pytest pytest-asyncio pytest-mock pytest-cov"
    exit 1
fi
echo -e "${GREEN}   ✓ pytest found${NC}"

# Check test file exists
echo ""
echo "3. Checking test file..."
if [ ! -f "tests/unit/test_orchestrator.py" ]; then
    echo -e "${RED}   ✗ test_orchestrator.py not found${NC}"
    exit 1
fi
echo -e "${GREEN}   ✓ test_orchestrator.py exists${NC}"

# Count tests
test_count=$(grep -c "^    def test_\|^    async def test_" tests/unit/test_orchestrator.py || true)
echo "   Test count: $test_count"

# List test classes
echo ""
echo "4. Test classes found:"
grep "^class Test" tests/unit/test_orchestrator.py | while read line; do
    echo "   - $line"
done

# Try to run tests
echo ""
echo "5. Running tests..."
echo "   Command: pytest tests/unit/test_orchestrator.py -v --tb=short"
echo ""

if pytest tests/unit/test_orchestrator.py -v --tb=short; then
    echo ""
    echo -e "${GREEN}==================================="
    echo "✓ All tests passed!"
    echo "===================================${NC}"
    
    # Run with coverage
    echo ""
    echo "6. Running coverage analysis..."
    pytest tests/unit/test_orchestrator.py \
        --cov=src/pipeline/orchestrator \
        --cov-report=term-missing \
        --cov-report=html \
        --tb=short
    
    echo ""
    echo -e "${GREEN}Coverage report generated in htmlcov/index.html${NC}"
    
    # Check if coverage meets threshold
    coverage_pct=$(pytest tests/unit/test_orchestrator.py \
        --cov=src/pipeline/orchestrator \
        --cov-report=term \
        --tb=no -q 2>&1 | grep "TOTAL" | awk '{print $4}' | sed 's/%//')
    
    if [ ! -z "$coverage_pct" ]; then
        echo "Coverage: ${coverage_pct}%"
        if (( $(echo "$coverage_pct >= 85" | bc -l) )); then
            echo -e "${GREEN}✓ Coverage threshold met (≥85%)${NC}"
        else
            echo -e "${YELLOW}⚠ Coverage below threshold (<85%)${NC}"
        fi
    fi
    
    # Check test duration
    echo ""
    echo "7. Checking test performance..."
    duration=$(pytest tests/unit/test_orchestrator.py -v --tb=no -q 2>&1 | grep "passed in" | grep -oP '\d+\.\d+s')
    if [ ! -z "$duration" ]; then
        echo "Test duration: $duration"
        # Extract seconds (remove 's')
        duration_num=$(echo $duration | sed 's/s//')
        if (( $(echo "$duration_num < 10" | bc -l) )); then
            echo -e "${GREEN}✓ Tests run in <10 seconds${NC}"
        else
            echo -e "${YELLOW}⚠ Tests slower than 10 seconds${NC}"
        fi
    fi
    
    echo ""
    echo -e "${GREEN}==================================="
    echo "✓ Verification complete!"
    echo "===================================${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}==================================="
    echo "✗ Some tests failed"
    echo "===================================${NC}"
    echo ""
    echo "Debug steps:"
    echo "1. Check dependencies: pip list | grep -E 'pytest|pydantic|azure|snowflake'"
    echo "2. Run single test: pytest tests/unit/test_orchestrator.py::TestPipelineMapping::test_init_with_valid_config_creates_mapping -v"
    echo "3. Check test file: cat tests/unit/test_orchestrator.py | head -50"
    exit 1
fi
