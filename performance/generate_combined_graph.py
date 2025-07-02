import pandas as pd
import matplotlib.pyplot as plt
import os

def generate_combined_chart(data, throughput_column, latency_column, output_dir):
    plt.figure(figsize=(12, 8))

    data = data.copy()
    data[throughput_column] = pd.to_numeric(data[throughput_column], errors='coerce')
    data[latency_column] = pd.to_numeric(data[latency_column], errors='coerce') / 1000  # Convert ms to seconds

    fig, ax1 = plt.subplots()

    # Throughput on the left y-axis
    ax1.bar(data['file'], data[throughput_column], color='#4CAF50', label='Throughput')
    ax1.set_ylabel('Throughput (tile/s)', color='#4CAF50')
    ax1.tick_params(axis='y', labelcolor='#4CAF50')

    # Latency on the right y-axis
    ax2 = ax1.twinx()
    ax2.plot(data['file'], data[latency_column], color='#03A9F4', marker='o', label='Latency')
    ax2.set_ylabel('Latency (s)', color='#03A9F4')
    ax2.tick_params(axis='y', labelcolor='#03A9F4')

    # Title and x-axis
    plt.title('Confronto prestazioni: Throughput e Latenza (160 Batches)')
    ax1.set_xlabel('Configurazione')
    plt.xticks(rotation=0, ha='center')

    # Rimuovi la legenda per evitare sovrapposizioni
    ax1.legend().remove()
    ax2.legend().remove()

    # Grid
    ax1.grid(axis='y', linestyle='--', alpha=0.7)

    # Save the figure
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'combined_throughput_latency.png'))
    plt.close()

def main():
    performance_dir = os.path.dirname(__file__)
    stats_file = os.path.join(performance_dir, 'performance_stats.csv')
    output_dir = os.path.join(performance_dir, 'graphs')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    data = pd.read_csv(stats_file)

    # Filter for Flink configurations
    flink_configs = data[data['file'].isin(['flink_base.txt', 'flink_base_2.txt', 'flink_base_4.txt'])]

    # Correggi SettingWithCopyWarning utilizzando .loc
    flink_configs.loc[:, 'file'] = flink_configs['file'].replace({
        'flink_base.txt': 'Flink (Par. 1)',
        'flink_base_2.txt': 'Flink (Par. 2)',
        'flink_base_4.txt': 'Flink (Par. 4)'
    })

    # Generate the combined chart
    generate_combined_chart(flink_configs, 'throughput_mean (tile/s)', 'latency_mean (ms)', output_dir)

if __name__ == '__main__':
    main()
