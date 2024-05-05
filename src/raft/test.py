import subprocess
import time
from multiprocessing import Pool

max_attempts = 300
success_count = 0
parallel_count = 30

def run_test(attempt):
    print("")
    print(f"Attempt {attempt}")

    start_time = time.time()

    process = subprocess.Popen(["go", "test", "-race"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    if process.returncode == 0:
        return True
    else:
        print("Test failed")
        return False

def main():
    pool = Pool(processes=parallel_count)
    attempts = range(1, max_attempts + 1)
    global success_count  # 声明成功测试数量为全局变量
    success_count = sum(pool.map(run_test, attempts))  # 统计成功的测试数量
    pool.close()
    pool.join()

    success_rate = (success_count / max_attempts) * 100
    print(f"Success rate: {success_rate}%")

if __name__ == "__main__":
    main()
