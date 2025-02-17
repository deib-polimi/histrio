import os

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import seaborn as sns
import re
import numpy as np

# matplotlib.use('TkAgg')

sns.set_theme(style="whitegrid", palette=sns.color_palette("colorblind"), context="paper", font_scale=1.5, rc={"lines.linewidth": 2}, font="serif")
# sns.set_style(context="paper")
def log_ticks(fig=None):
    # locmin = LogLocator(base=10, subs=np.arange(0.2,1,0.2), numticks=5)
    
    fig = fig or plt.gcf()
    
    for ax in fig.axes:
        ax.grid(True, which="major", c='lightgray')
        ax.grid(True, which="minor", c='whitesmoke')

def save_all_throughput_plots():
    folder_names = [name for name in os.listdir("./data") if os.path.isdir("./data/" + name)]
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
    dash_styles = ["solid", "dashed", "dashdot", (0, (7, 1)), (0, (5,1,1,1,1,1))]
    dash_style_index = 0
    plt.figure(figsize=(6, 3.5))
    for df_name, df in selected_datasets:
        label_name = df_name.split("-")[1]

        match = re.search(r"(\d+)ms", df_name, re.IGNORECASE)
        if match:
            offset_value = int(match.group(1))
            # df["offset"] = offset_value - (df["start_timestamp"].astype(np.int64) / 1000000) % offset_value
            df["offset"] = offset_value / 2
        else:
            df["offset"] = 0

        df["offset_response_time"] = df["response_time"] + df["offset"]

        if graph_type == "throughput":
            throughput_df = create_throughput_df(df)
            sns_plot = sns.lineplot(data=throughput_df, x="seconds", y="throughput", label=label_name, linestyle=dash_styles[dash_style_index], alpha=0.9)
            ylabel_name = "Throughput (req/s)"
        elif graph_type == "latency":
            latency_df = create_latency_df(df)
            sns_plot = sns.lineplot(data=latency_df, x="seconds", y="offset_response_time", label=label_name, linestyle=dash_styles[dash_style_index], alpha=0.9, estimator="mean", errorbar="sd")
            # if df["offset"].max() > 0:
            #     sns.lineplot(data=latency_df, x="seconds", y="offset", linestyle="dashed", alpha=0.4, color=sns_plot.get_lines()[-1].get_color(), linewidth=1.5)
            ylabel_name = "Latency (ms)"
        elif graph_type == "latency-log":
            latency_df = create_latency_df(df)
            sns_plot = sns.lineplot(data=latency_df, x="seconds", y="offset_response_time", label=label_name, linestyle=dash_styles[dash_style_index], alpha=0.9, estimator="mean", errorbar="sd")
            plt.semilogy()
            plt.gca().get_yaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
            log_ticks()
            ylabel_name = "Latency (ms)"
        else:
            raise RuntimeError(f"could not recognize type '{graph_type}'")
        dash_style_index = (dash_style_index + 1) % len(dash_styles)
    plt.legend(loc="upper right")
    plt.xlabel(xlabel_name)
    plt.ylabel(ylabel_name)

    base_dir = "comparison-graphs/"
    if not os.path.isdir(base_dir):
        os.makedirs(base_dir)

    fig = sns_plot.get_figure()
    fig.savefig(base_dir + file_name + ".pdf", bbox_inches='tight')
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


def aggregate_logs(folder_name, logs_count):
    dataframes = [pd.read_csv("data/" + folder_name + "/" + str(i) + ".log",
                              names=["request_id", "start_timestamp", "end_timestamp", "response_time"]) for i in
                  range(0, logs_count)]

    df = pd.concat(dataframes)
    df["start_timestamp"] = pd.to_datetime(df['start_timestamp'], unit='ms')
    df["end_timestamp"] = pd.to_datetime(df['end_timestamp'], unit='ms')
    df["second_bin"] = df.end_timestamp.apply(lambda x: datetime.datetime.fromtimestamp(int(x.timestamp())))

    return df


def create_throughput_df(df):
    throughput_df = df[["start_timestamp", "end_timestamp", "second_bin"]] \
        .groupby(df.second_bin) \
        .agg({"end_timestamp": "count"}) \
        .rename(columns={"end_timestamp": "throughput"})

    setup_seconds_column(throughput_df)
    return throughput_df


def create_latency_df(df):
    latency_df = df
    # df[["response_time", "end_timestamp", "second_bin"]] \
    #     .groupby(df.second_bin) \
    #     .agg({"response_time": "mean"}) \
    df.sort_values(by=["end_timestamp"], inplace=True)
    first_timestamp = df.iloc[0]["end_timestamp"]
    df["seconds"] = df["end_timestamp"].map(lambda x: np.floor(datetime.timedelta.total_seconds(x - first_timestamp)))
    
    return latency_df


def setup_seconds_column(df):
    df.sort_values(by=["second_bin"], inplace=True)
    first_timestamp = df.index[0]
    df["seconds"] = df.index.map(lambda x: datetime.timedelta.total_seconds(x - first_timestamp))


# df is a throughput df
def compute_average_throughput(df):
    delta_seconds = df["seconds"].max()
    total_requests = df["throughput"].sum()
    return total_requests / delta_seconds


# df is a latency df
def compute_average_latency(df):
    return df["response_time"].mean()


def get_dfs_by_name():
    dfs_by_name = {
        "BANKING_THROUGHPUT-1 Worker": aggregate_logs("SUT_TEST_SMALL_1WORKERS_5_CCA_BACKOFF", 5),
        "BANKING_THROUGHPUT-2 Workers": aggregate_logs("SUT_TEST_SMALL_2WORKERS_5_CCA_BACKOFF", 5),
        "BANKING_THROUGHPUT-4 Workers": aggregate_logs("SUT_TEST_SMALL_4WORKERS_5_CCA_BACKOFF", 5),
        "BANKING_THROUGHPUT-8 Workers": aggregate_logs("SUT_TEST_SMALL_8WORKERS_5_CCA_BACKOFF_AGAIN", 5),
        "BANKING_THROUGHPUT-Baseline": aggregate_logs("BASELINE_BANKING_30kACC_60kTX", 5),
        "BANKING_LATENCY-Baseline": aggregate_logs("BASELINE_BANKING_LATENCY_30k_ACC_WITH_RATE_LIMITER", 5),
        "HOTEL_THROUGHPUT-1 Worker": aggregate_logs("SUT_HOTEL_100H_200_U_10kTX_1_WORKER", 5),
        "HOTEL_THROUGHPUT-2 Workers": aggregate_logs("SUT_HOTEL_100H_200_U_10kTX_2_WORKER", 5),
        "HOTEL_THROUGHPUT-4 Workers": aggregate_logs("SUT_HOTEL_100H_200_U_10kTX_4_WORKER", 5),
        "HOTEL_THROUGHPUT-8 Workers": aggregate_logs("SUT_HOTEL_100H_200_U_10kTX_8_WORKER", 5),
        # the latency benchmarks for the sut have been run with 4 workers
        "HOTEL_LATENCY-100ms polling": aggregate_logs("SUT_HOTEL_100H_200_U_10TX_EVERY_2_SECONDS_100MS_POLL", 5),
        "HOTEL_LATENCY-500ms polling": aggregate_logs("SUT_HOTEL_100H_200_U_10TX_EVERY_2_SECONDS_500MS_POLL", 5),
        "HOTEL_LATENCY-1000ms polling": aggregate_logs("SUT_HOTEL_100H_200_U_10TX_EVERY_2_SECONDS_1000MS_POLL", 5),
        "BANKING_LATENCY-100ms polling": aggregate_logs("SUT_BANKING_SLOW_4_WORKERS_100MS_POLL", 5),
        "BANKING_LATENCY-500ms polling": aggregate_logs("SUT_BANKING_SLOW_4_WORKERS_500MS_POLL", 5),
        "BANKING_LATENCY-1000ms polling": aggregate_logs("SUT_BANKING_SLOW_4_WORKERS_1000MS_POLL", 5),
        "HOTEL_THROUGHPUT-Baseline": aggregate_logs("BASELINE_HOTEL_THROUHPUT_10k_REQS", 5),
        "HOTEL_LATENCY-Baseline": aggregate_logs("BASELINE_HOTEL_LATENCY_10k_100H_200U_WITH_RATE_LIMITER", 5),
        "HOTEL_THROUGHPUT_WITH_CONTENTION-Baseline": aggregate_logs("BASELINE_HOTEL_THROUHPUT_10k_100H_200U", 5),
        # "Baseline hotel latency with contention": aggregate_logs("BASELINE_HOTEL_LATENCY_10k_100H_200U", 5),
    }
    return dfs_by_name


def save_thesis_plots():
    save_plots_comparison(["BANKING_THROUGHPUT-Baseline", "BANKING_THROUGHPUT-1 Worker", "BANKING_THROUGHPUT-2 Workers", "BANKING_THROUGHPUT-4 Workers", "BANKING_THROUGHPUT-8 Workers"], "throughput", "Banking throughput", "Banking_throughput")
    save_plots_comparison(["BANKING_LATENCY-Baseline", "BANKING_LATENCY-100ms polling", "BANKING_LATENCY-500ms polling",
                           "BANKING_LATENCY-1000ms polling"], "latency", "Banking latency", "Banking_latency")
    save_plots_comparison(["BANKING_LATENCY-Baseline", "BANKING_LATENCY-100ms polling", "BANKING_LATENCY-500ms polling",
                        "BANKING_LATENCY-1000ms polling"], "latency-log", "Banking latency(log)", "Banking_latency_log")
    save_plots_comparison(["HOTEL_THROUGHPUT_WITH_CONTENTION-Baseline", "HOTEL_THROUGHPUT-1 Worker", "HOTEL_THROUGHPUT-2 Workers", "HOTEL_THROUGHPUT-4 Workers", "HOTEL_THROUGHPUT-8 Workers"], "throughput", "Hotel throughput", "Hotel_throughput")
    save_plots_comparison(["HOTEL_LATENCY-Baseline", "HOTEL_LATENCY-100ms polling", "HOTEL_LATENCY-500ms polling", "HOTEL_LATENCY-1000ms polling"], "latency", "Hotel latency", "Hotel_latency")
    save_plots_comparison(["HOTEL_LATENCY-Baseline", "HOTEL_LATENCY-100ms polling", "HOTEL_LATENCY-500ms polling", "HOTEL_LATENCY-1000ms polling"], "latency-log", "Hotel latency (log)", "Hotel_latency_log")


save_thesis_plots()

