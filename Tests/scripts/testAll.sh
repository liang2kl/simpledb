#!/usr/bin/env bash
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

NUM_TESTS=$(ls bin/*Test | wc -l | xargs)
NUM_PASSED=0

echo "Number of tests: $NUM_TESTS\n=================="

for target in bin/*Test; do
    echo "Running $target (#$((NUM_PASSED + 1)))..."
    if [[ -x $target ]]; then
        $target
        if [[ $? -eq 0 ]]; then
            ((NUM_PASSED++))
        fi
    else
        echo " ${RED}NOT AN EXECUTABLE${NC}"
    fi
done

echo "==================\nPassed $NUM_PASSED/$NUM_TESTS tests"
