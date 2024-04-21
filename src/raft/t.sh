#!/bin/bash

max_attempts=10
attempts=0
success_count=0

while true; do
    ((attempts++))
    echo ""
    echo "Attempt $attempts"

    start_time=$(date +%s%N)

    if go test -run TestFigure8Unreliable2C -race; then
        ((success_count++))
        end_time=$(date +%s%N) # 记录结束时间（纳秒）
        duration=$((end_time - start_time)) # 计算测试运行时间（纳秒）
        total_duration=$((total_duration + duration)) # 累加测试运行时间
    else
        echo "Test failed"
        break
    fi

    if [ $attempts -ge $max_attempts ]; then
        echo "Reached maximum attempts, stopping."
        break
    fi
done

average_duration=$(bc <<< "scale=2; $total_duration / $attempts / 1000000") # 计算平均运行时间（毫秒）
echo "Average duration: $average_duration milliseconds"

#success_rate=$(bc <<< "scale=2; $success_count / $attempts * 100")
#echo "Success rate: $success_rate%"
