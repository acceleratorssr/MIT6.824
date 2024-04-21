#!/bin/bash

max_attempts=10
attempts=0
success_count=0

while true; do
    ((attempts++))
    echo ""
    echo "Attempt $attempts"

    if go test -run TestFigure8Unreliable2C; then
        ((success_count++))
    else
        echo "Test failed"
    fi

    if [ $attempts -ge $max_attempts ]; then
        echo "Reached maximum attempts, stopping."
        break
    fi
done

success_rate=$(bc <<< "scale=2; $success_count / $attempts * 100")
echo "Success rate: $success_rate%"
