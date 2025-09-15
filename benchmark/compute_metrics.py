import os

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import seaborn as sns
import re
import numpy as np
import argparse
from glob import glob

# matplotlib.use('TkAgg')

sns.set_theme(
    style="whitegrid",
    palette=sns.color_palette("colorblind"),
    context="paper",
    font_scale=1.5,
    rc={"lines.linewidth": 2},
    font="serif",
)


# sns.set_style(context="paper")
def log_ticks(fig=None):
    # locmin = LogLocator(base=10, subs=np.arange(0.2,1,0.2), numticks=5)

    fig = fig or plt.gcf()

    for ax in fig.axes:
        ax.grid(True, which="major", c="lightgray")
        ax.grid(True, which="minor", c="whitesmoke")


def save_all_throughput_plots():
    folder_names = [
        name for name in os.listdir("./data") if os.path.isdir("./data/" + name)
    ]
    for folder_name in folder_names:
        df = aggregate_logs(folder_name, 5)

        histogram = create_throughput_df(df)

        sns_plot = sns.lineplot(data=histogram, x="seconds", y="throughput")
        sns_plot.set_title(folder_name)
        fig = sns_plot.get_figure()

        base_dir = "graphs/throughput/"
        if not os.path.isdir(base_dir):
            os.makedirs(base_dir)
        fig.savefig(base_dir + folder_name + ".pdf")
        plt.clf()


def plot_throughput_plots_compared():
    dfs_by_name = get_dfs_by_name()

    for label_name, df in dfs_by_name.items():
        throughput_df = create_throughput_df(df)
        sns.lineplot(data=throughput_df, x="seconds", y="throughput", label=label_name)
    plt.legend(loc="upper right")
    plt.show()


def save_plots_comparison(dataset_names, graph_type, graph_name, file_name):
    dfs_by_name = get_dfs_by_name()
    selected_datasets = [(name, dfs_by_name[name]) for name in dataset_names]
    sns_plot = None
    xlabel_name = "Time (s)"
    ylabel_name = ""
    dash_styles = ["solid", "dashed", "dashdot", (0, (7, 1)), (0, (5, 1, 1, 1, 1, 1))]
    dash_style_index = 0
    f, [ax1, ax2, ax3] = plt.subplots(ncols=3, figsize=(20, 5))
    for df_name, df in selected_datasets:
        label_name = df_name.split(" - ")[1]

        # match = re.search(r"(\d+)ms", df_name, re.IGNORECASE)
        # if match:
        #     offset_value = int(match.group(1))
        #     # df["offset"] = offset_value - (df["start_timestamp"].astype(np.int64) / 1000000) % offset_value
        #     df["offset"] = offset_value / 2
        # else:
        #     df["offset"] = 0

        # df["offset_response_time"] = df["response_time"]  # + df["offset"]

        if graph_type == "throughput":
            throughput_df = create_throughput_df(df)
            sns_plot = sns.lineplot(
                data=throughput_df,
                x="seconds",
                y="throughput",
                label=label_name,
                linestyle=dash_styles[dash_style_index],
                alpha=0.9,
            )
            ylabel_name = "Throughput (req/s)"
        elif graph_type == "latency":
            latency_df = create_latency_df(df)
            sns_plot = sns.lineplot(
                data=latency_df,
                x="seconds",
                y="response_time",
                label=label_name,
                linestyle=dash_styles[dash_style_index],
                alpha=0.9,
                estimator="median",
                errorbar="pi",
                ax=ax1,
            )
            sns.kdeplot(
                data=latency_df,
                x="response_time",
                label=label_name,
                linestyle=dash_styles[dash_style_index],
                cut=0,
                fill=True,
                clip=(0, None),
                linewidth=2,
                alpha=0.1,
                ax=ax2,
            )
            sns.kdeplot(
                data=latency_df,
                x="response_time",
                label=label_name,
                linestyle=dash_styles[dash_style_index],
                cut=1,
                bw_adjust=0.5,
                alpha=0.9,
                clip=(0, None),
                cumulative=True,
                linewidth=2,
                ax=ax3,
            )
            # if df["offset"].max() > 0:
            #     sns.lineplot(data=latency_df, x="seconds", y="offset", linestyle="dashed", alpha=0.4, color=sns_plot.get_lines()[-1].get_color(), linewidth=1.5)
            ylabel_name = "Latency (ms)"
        elif graph_type == "latency-log":
            latency_df = create_latency_df(df)
            sns_plot = sns.lineplot(
                data=latency_df,
                x="seconds",
                y="response_time",
                label=label_name,
                linestyle=dash_styles[dash_style_index],
                alpha=0.9,
                estimator="median",
                errorbar="pi",
            )
            plt.semilogy()
            plt.gca().get_yaxis().set_major_formatter(
                matplotlib.ticker.ScalarFormatter()
            )
            log_ticks()
            ylabel_name = "Latency (ms)"
        else:
            raise RuntimeError(f"could not recognize type '{graph_type}'")
        dash_style_index = (dash_style_index + 1) % len(dash_styles)
    ax1.legend(bbox_to_anchor=(0.5, -0.25), loc="upper center")
    ax2.legend(bbox_to_anchor=(0.5, -0.25), loc="upper center")
    ax3.legend(bbox_to_anchor=(0.5, -0.25), loc="upper center")
    ax1.xaxis.set_label(xlabel_name)
    ax1.yaxis.set_label(ylabel_name)

    base_dir = "comparison-graphs/"
    if not os.path.isdir(base_dir):
        os.makedirs(base_dir)

    fig = sns_plot.get_figure()
    fig.savefig(base_dir + file_name + ".pdf", bbox_inches="tight")
    plt.clf()
    # plt.show()


def save_all_latency_plots():
    dfs_by_name = get_dfs_by_name()

    for label_name, df in dfs_by_name.items():
        latency_df = create_latency_df(df)
        sns_plt = sns.lineplot(data=latency_df, x="seconds", y="response_time")
        sns_plt.set_title(label_name)
        plt.ylabel("Latency (ms)")
        mean_latency = df["response_time"].mean()
        y_limit = max(200, mean_latency * 2)
        ax = plt.gca()
        ax.set_ylim([0, y_limit])
        fig = sns_plt.get_figure()

        base_dir = "graphs/latency/"
        if not os.path.isdir(base_dir):
            os.makedirs(base_dir)
        fig.savefig("graphs/latency/" + label_name + ".pdf")
        plt.clf()

        # plt.show()


def aggregate_logs(folder_name):

    dataframes = [
        pd.read_csv(
            df,
            names=["request_id", "start_timestamp", "end_timestamp", "response_time"],
        )
        for df in glob(f"data/{folder_name}/*.log")
    ]

    df = pd.concat(dataframes)
    df["start_timestamp"] = pd.to_datetime(df["start_timestamp"], unit="ms")
    df["end_timestamp"] = pd.to_datetime(df["end_timestamp"], unit="ms")
    df["second_bin"] = df.end_timestamp.apply(
        lambda x: datetime.datetime.fromtimestamp(int(x.timestamp()))
    )

    return df


def create_throughput_df(df):
    throughput_df = (
        df[["start_timestamp", "end_timestamp", "second_bin"]]
        .groupby(df.second_bin)
        .agg({"end_timestamp": "count"})
        .rename(columns={"end_timestamp": "throughput"})
    )

    setup_seconds_column(throughput_df)
    return throughput_df


def create_latency_df(df):
    latency_df = df
    # df[["response_time", "end_timestamp", "second_bin"]] \
    #     .groupby(df.second_bin) \
    #     .agg({"response_time": "mean"}) \
    df.sort_values(by=["end_timestamp"], inplace=True)
    first_timestamp = df.iloc[0]["end_timestamp"]
    df["seconds"] = df["end_timestamp"].map(
        lambda x: np.floor(datetime.timedelta.total_seconds(x - first_timestamp))
    )

    return latency_df


def setup_seconds_column(df):
    df.sort_values(by=["second_bin"], inplace=True)
    first_timestamp = df.index[0]
    df["seconds"] = df.index.map(
        lambda x: datetime.timedelta.total_seconds(x - first_timestamp)
    )


# df is a throughput df
def compute_average_throughput(df):
    delta_seconds = df["seconds"].max()
    total_requests = df["throughput"].sum()
    return total_requests / delta_seconds


# df is a latency df
def compute_average_latency(df):
    return df["response_time"].mean()


def define_experiments():
    experiments = {
        # HISTRIO2 LATENCY experiments
        "b100-noq-0": {
            "display_name": "100ms",
            "type": "latency",
        },
        "b100-mq-0": {
            "display_name": "100ms + MQ",
            "type": "latency",
        },
        "b250-noq-0": {
            "display_name": "250ms",
            "type": "latency",
        },
        "b250-mq-0": {
            "display_name": "250ms + MQ",
            "type": "latency",
        },
        "b1000-noq-0": {
            "display_name": "1000ms",
            "type": "latency",
        },
        "b1000-mq-0": {
            "display_name": "1000ms + MQ",
            "type": "latency",
        },
        "h100-noq-0": {
            "display_name": "100ms",
            "type": "latency",
        },
        "h100-mq-0": {
            "display_name": "100ms + MQ",
            "type": "latency",
        },
        "h250-noq-0": {
            "display_name": "250ms",
            "type": "latency",
        },
        "h250-mq-0": {
            "display_name": "250ms + MQ",
            "type": "latency",
        },
        "h1000-noq-0": {
            "display_name": "1000ms",
            "type": "latency",
        },
        "h1000-mq-0": {
            "display_name": "1000ms + MQ",
            "type": "latency",
        }
    }
    return experiments


def get_dfs_by_name(experiment_ids=None):
    """Load dataframes for selected experiments"""
    experiments = define_experiments()

    # If no experiments specified, use all
    if experiment_ids is None:
        experiment_ids = list(experiments.keys())

    dfs_by_name = {}
    for exp_id in experiment_ids:
        if exp_id not in experiments:
            print(f"Warning: Experiment '{exp_id}' not found. Skipping.")
            continue

        exp = experiments[exp_id]
        df = aggregate_logs(exp_id)
        display_name = f"{exp_id} - {exp['display_name']}"
        dfs_by_name[display_name] = df

    return dfs_by_name


def save_thesis_plots(experiments=None, output_filename=None):
    """
    Save plots for selected experiments.

    Args:
        experiments: List of experiment IDs to include, or None for all
        output_filename: Base name for output file (without extension)
    """
    if experiments is None:
        print("select experiments with -e")
        # Default to histrio2 experiments
        exit(-2)

    # Get display names for the experiments
    all_experiments = define_experiments()
    display_names = [
        f"{exp_id} - {all_experiments[exp_id]['display_name']}"
        for exp_id in experiments
        if exp_id in all_experiments
    ]

    # Default output filename if none provided
    if output_filename is None:
        output_filename = "experiment_comparison"

    # Load data and create plots
    save_plots_comparison(
        display_names, "latency", "Latency Comparison", output_filename
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate thesis plots for selected experiments"
    )
    parser.add_argument(
        "--experiments",
        "-e",
        nargs="*",
        help="List of experiment IDs to include (default: all histrio2 experiments)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="experiment_comparison",
        help="Base name for output file (default: experiment_comparison)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    save_thesis_plots(experiments=args.experiments, output_filename=args.output)
