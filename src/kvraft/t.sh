#!/bin/bash

max_attempts=10
attempts=0
num_parallel_tests=1

# 创建临时文件来存储成功计数和总持续时间
success_file=$(mktemp)
duration_file=$(mktemp)
echo 0 > $success_file
echo 0 > $duration_file

run_test() {
    local attempt=$1
    local test_num=$2

    echo "Test $test_num - Attempt $attempt"

    start_time=$(date +%s%N)

    if go test -race; then
        end_time=$(date +%s%N) # 记录结束时间（纳秒）
        duration=$((end_time - start_time)) # 计算测试运行时间（纳秒）

        # 使用原子操作更新成功计数和总持续时间
        (
            flock -x 200
            success_count=$(cat $success_file)
            ((success_count++))
            echo $success_count > $success_file

            total_duration=$(cat $duration_file)
            total_duration=$((total_duration + duration))
            echo $total_duration > $duration_file
        ) 200>lockfile
    else
        echo "Test $test_num - Attempt $attempt failed"
    fi
}

while [ $attempts -lt $max_attempts ]; do
    ((attempts++))
    echo ""
    echo "Attempt $attempts"

    for ((i=0; i<$num_parallel_tests; i++)); do
        run_test $attempts $((i+1)) &
    done

    wait  # 等待所有后台任务完成

    if [ $attempts -ge $max_attempts ]; then
        echo "Reached maximum attempts, stopping."
        break
    fi
done

# 读取最终的成功计数和总持续时间
success_count=$(cat $success_file)
total_duration=$(cat $duration_file)

# 删除临时文件
rm -f $success_file $duration_file

average_duration=$(bc <<< "scale=2; $total_duration / $success_count / 1000000") # 计算平均运行时间（毫秒）
echo "Average duration: $average_duration milliseconds"

success_rate=$(bc <<< "scale=2; $success_count / $attempts * 100 / $num_parallel_tests")
echo "Success rate: $success_rate%"
