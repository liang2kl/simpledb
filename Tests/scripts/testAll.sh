#!/usr/bin/env bash
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

NUM_TESTS=$(ls bin/*Test | wc -l | xargs)
NUM_PASSED=0

echo "Number of tests: $NUM_TESTS\n=================="

for target in bin/*Test; do
    printf '%s' "Running $target..."
    if [[ -x $target ]]; then
        $target
        if [[ $? -ne 0 ]]; then
            echo " ${RED}failed${NC}"
        else
            echo " ${GREEN}passed${NC}"
            ((NUM_PASSED++))
        fi
    else
        echo " ${RED}NOT AN EXECUTABLE!${NC}"
    fi
done

echo "==================\nPassed $NUM_PASSED/$NUM_TESTS tests"
