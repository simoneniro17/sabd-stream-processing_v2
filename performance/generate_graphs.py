import pandas as pd
import matplotlib.pyplot as plt
import os

def generate_bar_chart(data, metric_mean, metric_stddev, output_dir, title_suffix):
    plt.figure(figsize=(12, 8))

    data = data.copy()
    # Modifica i nomi delle configurazioni
    data['file'] = data['file'].replace({
        'baseline.txt': 'Baseline',
        'flink_base.txt': 'Flink (1)',
        'flink_base_2.txt': 'Flink (2)',
        'flink_base_4.txt': 'Flink (4)',
        'flink_base_short.txt': 'Flink (short)',
        'kafka_streams.txt': 'Kafka Streams'
    })

    # Normalizza i nomi delle colonne per rimuovere spazi
    data.columns = data.columns.str.strip()

    # Scala la Y per le latenze in secondi
    if 'latency' in metric_mean:
        data = data.copy()
        data[metric_mean] = pd.to_numeric(data[metric_mean], errors='coerce') / 1000
        data[metric_stddev] = pd.to_numeric(data[metric_stddev], errors='coerce') / 1000
        unit = ' (s)'
    else:
        unit = ' (tile/s)'

    # Aggiorna i colori per Throughput, Latenza e P99
    if 'throughput' in metric_mean:
        ax = data.plot(x='file', y=metric_mean, kind='bar', legend=False, color='#2CA02C', label='Media Throughput')
        data.plot(x='file', y=metric_stddev, kind='bar', legend=False, color='#98DF8A', ax=ax, alpha=0.8, label='Dev. std. Throughput')
    elif 'latency' in metric_mean:
        ax = data.plot(x='file', y=metric_mean, kind='bar', legend=False, color='#1F77B4', label='Media Latenza')
        data.plot(x='file', y=metric_stddev, kind='bar', legend=False, color='#AEC7E8', ax=ax, alpha=0.8, label='Dev. std. Latenza')

    plt.title(f'Confronto prestazioni: {metric_mean.split("_")[0].capitalize()} (160 Batches)')
    plt.ylabel(f'{metric_mean.split("_")[0].capitalize()}{unit}')
    plt.xlabel('Configurazione')
    plt.xticks(rotation=0, ha='center')
    plt.legend(['Media', 'Dev. std.'])

    # Griglia di sfondo
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Nomi file più corti
    file_suffix = title_suffix.split()[0].lower()
    if 'short' in title_suffix.lower():
        file_suffix = f'{metric_mean.split("_")[0]}_short'
    elif 'baseline' in title_suffix.lower():
        file_suffix = f'{metric_mean.split("_")[0]}_baseline'
    elif 'configurazioni' in title_suffix.lower():
        file_suffix = f'{metric_mean.split("_")[0]}_configurations'

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'{file_suffix}.png'))
    plt.close()

def generate_p99_chart(data, p99_column, output_dir, title_suffix):
    plt.figure(figsize=(12, 8))

    data = data.copy()
    data['file'] = data['file'].replace({
        'baseline.txt': 'Baseline',
        'flink_base.txt': 'Flink (Par. 1)',
        'flink_base_2.txt': 'Flink (Par. 2)',
        'flink_base_4.txt': 'Flink (Par. 4)',
        'flink_base_short.txt': 'Flink (short)',
        'kafka_streams.txt': 'Kafka Streams'
    })

    data[p99_column] = pd.to_numeric(data[p99_column], errors='coerce') / 1000

    # Cambia i colori di P99 per differenziarli maggiormente dal throughput
    ax = data.plot(x='file', y=p99_column, kind='bar', legend=False, color='#1F77B4', label='Media P99')
    p99_stddev_column = p99_column.replace('mean', 'stddev')
    data[p99_stddev_column] = pd.to_numeric(data[p99_stddev_column], errors='coerce') / 1000
    data.plot(x='file', y=p99_stddev_column, kind='bar', legend=False, color='#AEC7E8', ax=ax, alpha=0.8, label='Dev. std. P99')

    plt.title(f'Confronto prestazioni: Latency P99 (160 Batches)')
    plt.ylabel('Latency P99 (s)')
    plt.xlabel('Configurazione')
    plt.xticks(rotation=0, ha='center')
    plt.legend(['Media', 'Dev. std.'])

    # Aggiungi griglia di sfondo
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Nomi file più corti
    file_suffix = f'p99_latency_{title_suffix.split()[0].lower()}'
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'{file_suffix}.png'))
    plt.close()

def main():
    performance_dir = os.path.dirname(__file__)
    stats_file = os.path.join(performance_dir, 'performance_stats.csv')
    output_dir = os.path.join(performance_dir, 'graphs')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    data = pd.read_csv(stats_file)

    # Grafico per confrontare Baseline e Flink Base
    baseline_vs_flink_base = data[data['file'].isin(['baseline.txt', 'flink_base.txt'])]
    metrics = [('throughput_mean (tile/s)', 'throughput_stddev (tile/s)'),
               ('latency_mean (ms)', 'latency_stddev (ms)')]
    for metric_mean, metric_stddev in metrics:
        generate_bar_chart(baseline_vs_flink_base, metric_mean, metric_stddev, output_dir, 'Baseline vs Flink Base')

    # Grafico per confrontare Flink Base (short) e Kafka Streams
    flink_short_kafka = data[data['file'].isin(['flink_base_short.txt', 'kafka_streams.txt'])]
    for metric_mean, metric_stddev in metrics:
        generate_bar_chart(flink_short_kafka, metric_mean, metric_stddev, output_dir, 'Flink Base (short) vs Kafka Streams')

    # Grafico per confrontare le configurazioni di Flink Base
    flink_configs = data[data['file'].isin(['flink_base.txt', 'flink_base_2.txt', 'flink_base_4.txt'])]
    for metric_mean, metric_stddev in metrics:
        generate_bar_chart(flink_configs, metric_mean, metric_stddev, output_dir, 'Configurazioni Flink Base')
        if 'latency' in metric_mean:
            p99_column = metric_mean.replace('mean', 'p99_mean')
            generate_p99_chart(baseline_vs_flink_base, p99_column, output_dir, 'Baseline vs Flink Base')
            generate_p99_chart(flink_short_kafka, p99_column, output_dir, 'Flink Base (short) vs Kafka Streams')
            generate_p99_chart(flink_configs, p99_column, output_dir, 'Configurazioni Flink Base')

if __name__ == '__main__':
    main()
