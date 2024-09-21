import datetime

'''
	startTime := time.Now()

	defer func() {
		elapsed := time.Since(startTime)

		f, _ := os.OpenFile("execution_times.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer f.Close()

		if _, err := f.WriteString(fmt.Sprintf("%s\n", elapsed)); err != nil {
			fmt.Println("Error writing to file:", err)
		}
	}()
'''

def parse_duration(duration_str):
    try:
        duration_str = duration_str.replace('µs', 'us')

        if 'us' in duration_str:
            return float(duration_str.replace('us', '').strip())
        elif 'ms' in duration_str:
            return float(duration_str.replace('ms', '').strip()) * 1000
    except ValueError:
        print(f"Error parsing: {duration_str}")
    return None

def find_max_duration(file_path):
    max_duration = 0.0

    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            duration = parse_duration(line)

            if duration is not None and duration > max_duration:
                max_duration = duration

    return max_duration

if __name__ == "__main__":
    file_path = "execution_times.txt"

    max_duration = find_max_duration(file_path)

    print(f"The longest: {max_duration} us")
