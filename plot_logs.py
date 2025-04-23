# import pandas as pd
# import matplotlib.pyplot as plt

# # ----------------------------------------------------------------------
# # 1) DEFINE ALL FILE PATHS IN ONE DICTIONARY
# # ----------------------------------------------------------------------
# file_paths = {
#     "download": "timed_log_download.csv",
#     "write":  "timed_log_io.csv",
# }

# # ----------------------------------------------------------------------
# # 2) LOAD CSVs INTO A SINGLE DICT
# # ----------------------------------------------------------------------
# data = {}
# for key, path in file_paths.items():
#     try:
#         data[key] = pd.read_csv(
#             path,
#             header=None,
#             names=["current_time", "time_since_beginning", "throughputs", "threads"]
#         )
#     except FileNotFoundError:
#         print(f"File not found: {path}")
#         data[key] = pd.DataFrame()

# # ----------------------------------------------------------------------
# # 3) SETUP FIGURE & SUBPLOTS
# # ----------------------------------------------------------------------
# fig, (ax_net, ax_write) = plt.subplots(1, 2, figsize=(12, 5))

# # -------------------------
# # LEFT SUBPLOT: download DUAL-AXIS
# # -------------------------
# df_net = data["download"]
# if not df_net.empty:
#     # rolling averages
#     time_idx = df_net.index
#     conc_avg = df_net["threads"].rolling(window=5).mean()
#     thr_avg  = df_net["throughputs"].rolling(window=5).mean()

#     # plot concurrency on left y-axis
#     l1, = ax_net.plot(
#         time_idx, conc_avg,
#         linestyle="--", color="green",
#         label="Concurrency"
#     )
#     ax_net.set_xlabel("Sample Index")
#     ax_net.set_ylabel("Concurrency", color="green")
#     ax_net.tick_params(axis="y", labelcolor="green")
#     ax_net.grid(True, linestyle=":", alpha=0.5)

#     # plot throughput on right y-axis
#     ax2 = ax_net.twinx()
#     l2, = ax2.plot(
#         time_idx, thr_avg,
#         linestyle="-.", color="red",
#         label="Throughput"
#     )
#     ax2.set_ylabel("Throughput (Mbps)", color="red")
#     ax2.tick_params(axis="y", labelcolor="red")

#     # legend
#     ax_net.legend([l1, l2], [l1.get_label(), l2.get_label()], loc="upper left", fontsize=8)
#     ax_net.set_title("Download")
# else:
#     ax_net.text(0.5, 0.5, "download data not found", ha="center", va="center")
#     ax_net.set_title("Download")

# # -------------------------
# # RIGHT SUBPLOT: write DUAL-AXIS
# # -------------------------
# df_write = data["write"]
# if not df_write.empty:
#     t = df_write["time_since_beginning"]
#     thr = df_write["throughputs"].rolling(window=5).mean()
#     conc = df_write["threads"].rolling(window=5).mean()

#     # throughput on left y-axis
#     f1, = ax_write.plot(t, thr, color="red", label="Throughput")
#     ax_write.set_xlabel("Duration (s)")
#     ax_write.set_ylabel("Throughput (Mbps)", color="red")
#     ax_write.tick_params(axis="y", labelcolor="red")
#     ax_write.set_xlim(0, t.max())
    
#     # concurrency on right y-axis
#     ax3 = ax_write.twinx()
#     f2, = ax3.plot(t, conc, color="green", linestyle="--", label="Concurrency")
#     ax3.set_ylabel("Concurrency", color="green")
#     ax3.tick_params(axis="y", labelcolor="green")
#     ax3.set_ylim(0, conc.max() * 1.1)

#     # legend
#     ax_write.legend([f1, f2], [f1.get_label(), f2.get_label()], loc="best", fontsize=8)
#     ax_write.set_title("Write")
#     ax_write.grid(True, linestyle=":", alpha=0.5)
# else:
#     ax_write.text(0.5, 0.5, "write data not found", ha="center", va="center")
#     ax_write.set_title("Write")

# plt.tight_layout()
# plt.savefig("dual_axis_combined.pdf", dpi=300)
# plt.show()

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# ----------------------------------------------------------------------
# 1) DEFINE ALL FILE PATHS IN ONE DICTIONARY
# ----------------------------------------------------------------------
file_paths = {
    "download": "timed_log_download.csv",
    "write":    "timed_log_io.csv",
}

# ----------------------------------------------------------------------
# 2) LOAD CSVs INTO A SINGLE DICT
# ----------------------------------------------------------------------
data = {}
for key, path in file_paths.items():
    try:
        data[key] = pd.read_csv(
            path,
            header=None,
            names=["current_time", "time_since_beginning", "throughputs", "threads"]
        )
    except FileNotFoundError:
        print(f"File not found: {path}")
        data[key] = pd.DataFrame()

# Set seaborn style
sns.set_style("whitegrid")

# ----------------------------------------------------------------------
# 3) COMPUTE ROLLING AVERAGES
# ----------------------------------------------------------------------
for key, df in data.items():
    if not df.empty:
        df["threads_avg"] = df["threads"].rolling(window=5).mean()
        df["thr_avg"] = df["throughputs"].rolling(window=5).mean()

# ----------------------------------------------------------------------
# 4) PLOT WITH DUAL AXES USING SEABORN
# ----------------------------------------------------------------------
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# ---- Left: Download ----
df_d = data["download"]
if not df_d.empty:
    # Concurrency
    sns.lineplot(
        x=df_d.index,
        y="threads_avg",
        data=df_d,
        ax=ax1,
        label="Concurrency",
        color="navy"
    )
    ax1.set_xlabel("Sample Index")
    ax1.set_ylabel("Concurrency")
    ax1.set_title("Download")
    
    # Throughput on twin axis
    ax1b = ax1.twinx()
    sns.lineplot(
        x=df_d.index,
        y="thr_avg",
        data=df_d,
        ax=ax1b,
        label="Throughput",
        color="orange"
    )
    ax1b.set_ylabel("Throughput (Mbps)")

# ---- Right: Write ----
df_w = data["write"]
if not df_w.empty:
    # Concurrency
    sns.lineplot(
        x="time_since_beginning",
        y="threads_avg",
        data=df_w,
        ax=ax2,
        label="Concurrency",
        color="navy"
    )
    ax2.set_xlabel("Duration (s)")
    ax2.set_ylabel("Concurrency")
    ax2.set_title("Write")
    
    # Throughput on twin axis
    ax2b = ax2.twinx()
    sns.lineplot(
        x="time_since_beginning",
        y="thr_avg",
        data=df_w,
        ax=ax2b,
        label="Throughput",
        color="orange"
    )
    ax2b.set_ylabel("Throughput (Mbps)")

plt.tight_layout()
plt.savefig("dual_axis_seaborn.pdf", dpi=300)
plt.show()