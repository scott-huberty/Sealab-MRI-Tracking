import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set_theme(style="darkgrid")


def count_scans(df):
    # Grouped bar chart of Scan counts, X axis Scan type, Hue by visit,
    drop_these_cols = ["T1w", "T2w", "Status", "Reason Not-Acquired"]
    to_drop = [("Acquired", visit, scan) for visit in ["Newborn", "Six Months"] for scan in drop_these_cols]
    data = df.drop(columns=to_drop)
    data = data.drop(columns=[("Processed")])
    data = data.drop(columns=[("Acquired", "Newborn", "Biological Sex")])
    with pd.option_context("future.no_silent_downcasting", True):
        data = data.replace("Not Acquired", False)
        data = data.replace("Acquired", True)
    data = data.T
    data = (data.groupby(level=[0, 1, 2])
                .sum()
                .sum(axis=1)
                .to_frame(name="Count")
                )
    return data

def count_processed_scans(df):
    drop_these_cols = ["Surface-Recon-Method", "Date-Processed", "Precomputed", "Recon-all"]
    to_drop = [("Processed", visit, col)
               for visit in ["Newborn", "Six Months"]
               for col in drop_these_cols
               ]
    data = df.drop(columns=to_drop)
    data = data.drop(columns=[("Acquired")])
    with pd.option_context("future.no_silent_downcasting", True):
        data = data.replace("Not Processed", False)
        data = data.replace("N/A", False)
        data = data.replace("Processed", True)

    data = (data.T
                .groupby(by=["Visit", "Scan"])
                .sum()
                .sum(axis=1)
                .to_frame(name="Count")
              )
    # data = data.to_frame().stack().reset_index().rename(columns={0: "Value"})
    return data

def plot_scan_counts(data, ax):
    fig = ax.get_figure()

    with sns.color_palette("pastel"):
        sns.barplot(x="Scan", y="Count", hue="Visit", data=data, ax=ax)

    data.to_csv("./reports/scan_counts.csv")

def plot_processed_scan_counts(data, ax):
    fig = ax.get_figure()

    with sns.color_palette("muted"):
        sns.barplot(
            x="Scan",
            y="Count", 
            hue="Visit", 
            data=data, 
            ax=ax,
            linewidth=2.5,
            edgecolor=".5",
            )

    data.to_csv("./reports/processed_scans.csv")

def make_scan_bar_chart(show=True, save=True):
    fig, ax = plt.subplots(figsize=(10, 5), constrained_layout=True)
    ax.set_title("How many BABIES Scans have been Acquired and Processed?")

    data = count_scans(df)
    if save:
        data.to_csv("./reports/scan_counts.csv")
    plot_scan_counts(data, ax)
    data = count_processed_scans(df)
    if save:
        data.to_csv("./reports/processed_scans.csv")

    plot_processed_scan_counts(data, ax)
    # adjust legend
    handles, labels = ax.get_legend_handles_labels()
    new_labels = ["Newborn (Acquired)", "6mo (Acquired)", "Newborn (Processed)", "6mo (Processed)"]
    ax.legend(handles=handles, labels=new_labels, title="Visit")

    if save:
        fig.savefig("./reports/scan_counts.png")
    if show:
        return fig.show()

df = pd.read_csv("./reports/BABIES.csv", header=[0, 1, 2], index_col=0, keep_default_na=False)
make_scan_bar_chart()