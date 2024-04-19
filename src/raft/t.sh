#!/bin/bash

max_attempts=30
attempts=0

while true; do
    ((attempts++))
    echo ""
    echo "Attempt $attempts"

    if ! go test -run TestFailNoAgree2B; then
        echo "Test failed, stopping."
        break
    fi

    if [ $attempts -ge $max_attempts ]; then
        echo "Reached maximum attempts, stopping."
        break
    fi
done
