import os
import json
import csv
import statistics
import re

def parse_latency(latency_str):
    try:
        # Rimuove caratteri non validi
        latency_str = latency_str.replace('Â', '').replace('\u00b5', 'µ')

        # Usa regex per estrarre i valori numerici
        match = re.match(r"(\d+)s(?:([0-9]+)ms)?(?:([0-9]+)µs)?(?:([0-9]+)ns)?", latency_str)
        if not match:
            raise ValueError(f"Invalid latency format: {latency_str}")

        seconds = int(match.group(1))
        milliseconds = int(match.group(2)) if match.group(2) else 0
        microseconds = int(match.group(3)) if match.group(3) else 0
        nanoseconds = int(match.group(4)) if match.group(4) else 0

        total_ms = seconds * 1000 + milliseconds + microseconds / 1000 + nanoseconds / 1_000_000
        return total_ms
    except Exception as e:
        raise ValueError(f"Error parsing latency: {latency_str}, {e}")

def calculate_stats(file_path):
    with open(file_path, 'r') as f:
        data = [json.loads(line) for line in f]

    throughput_values = [record['throughput'] for record in data]
    latency_mean_values = [parse_latency(record['latency_mean']) for record in data]
    latency_p99_values = [parse_latency(record['latency_p99']) for record in data]

    stats = {
        'file': os.path.basename(file_path),

        'throughput_mean (tile/s)': round(statistics.mean(throughput_values), 6),
        'throughput_stddev (tile/s)': round(statistics.stdev(throughput_values), 6),
        'throughput_max (tile/s)': round(max(throughput_values), 6),
        'throughput_min (tile/s)': round(min(throughput_values), 6),

        'latency_mean (ms)': round(statistics.mean(latency_mean_values), 6),
        'latency_stddev (ms)': round(statistics.stdev(latency_mean_values), 6),
        'latency_max (ms)': round(max(latency_mean_values), 6),
        'latency_min (ms)': round(min(latency_mean_values), 6),

        'latency_p99_mean (ms)': round(statistics.mean(latency_p99_values), 6),
        'latency_p99_stddev (ms)': round(statistics.stdev(latency_p99_values), 6),
        'latency_p99_max (ms)': round(max(latency_p99_values), 6),
        'latency_p99_min (ms)': round(min(latency_p99_values), 6),
    }
    return stats

def main():
    performance_dir = os.path.dirname(__file__)
    output_file = os.path.join(performance_dir, 'performance_stats.csv')

    files = [f for f in os.listdir(performance_dir) if f.endswith('.txt')]

    all_stats = []
    for file in files:
        file_path = os.path.join(performance_dir, file)
        stats = calculate_stats(file_path)
        all_stats.append(stats)

    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['file',
                      'throughput_mean (tile/s)', 'throughput_stddev (tile/s)', 'throughput_max (tile/s)', 'throughput_min (tile/s)',
                      'latency_mean (ms)', 'latency_stddev (ms)', 'latency_max (ms)', 'latency_min (ms)',
                      'latency_p99_mean (ms)', 'latency_p99_stddev (ms)', 'latency_p99_max (ms)', 'latency_p99_min (ms)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(all_stats)

if __name__ == '__main__':
    main()
