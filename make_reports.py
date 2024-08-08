import argparse

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
    data = (data.groupby(by=["Visit", "Scan"]) # changed from level=[0, 1, 2]
                .sum()
                .sum(axis=1)
                .to_frame(name="Count")
                )
    return data

def count_all_scans(df, save=True):
    scan_counts = count_scans(df)
    proc_counts = count_processed_scans(df)
    scan_counts.name = "Acquired"
    proc_counts.name = "Processed"
    all_counts = pd.concat([scan_counts, proc_counts], keys=["Acquired", "Processed"], axis=1)
    # Now calculate the total of processed functional
    want_cols = ("Processed", "Count")
    want_indices_newborn = [("Newborn", scan) for scan in ["Functional-Volume", "Functional-Surface"]]
    want_indices_sixmonth = [("Six Months", scan) for scan in ["Functional-Volume", "Functional-Surface"]]
    functional_counts_newborn = all_counts.loc[want_indices_newborn, want_cols].sum()
    functional_counts_sixmonth = all_counts.loc[want_indices_sixmonth, want_cols].sum()
    all_counts.at[("Newborn", "Functional"), "Processed"] = functional_counts_newborn
    all_counts.at[("Six Months", "Functional"), "Processed"] = functional_counts_sixmonth
    if save:
        all_counts.to_csv("./reports/all_scan_counts.csv")
    return all_counts


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

def count_surface_recons(df):
    want_cols = [("Processed", age, "Surface-Recon-Method") for age in ("Newborn", "Six Months")]
    data = df.copy()[want_cols]
    with pd.option_context("future.no_silent_downcasting", True):
        data = data.replace("N/A", False)
        data = data.replace("Not Processed", False)
        # Now, how many values of "infantfs" do we have for each age?
        infantfs = data.T.groupby(by=["Visit", "Scan"]).apply(lambda x: x[x == "infantfs"].count()).sum(axis=1)
        infantfs.name = "infantfs"
        # rename index to "infantfs"
        infantfs.index.names = ["Visit", "Surface-Recon-Method"]
        # drop index level "Surface-Recon-Method"
        infantfs = infantfs.droplevel(1)

        mcribs = data.T.groupby(by=["Visit", "Scan"]).apply(lambda x: x[x == "mcribs"].count()).sum(axis=1)
        mcribs.name = "mcribs"
        mcribs.index.names = ["Visit", "Surface-Recon-Method"]
        mcribs = mcribs.droplevel(1)

        counts_df = pd.concat([infantfs, mcribs], axis=1)
        return counts_df

    data = data.T.groupby(level=[0, 1]).sum().sum(axis=1).to_frame(name="Count")

def custom_barchart_mpl(df, save=True):
    import numpy as np
    from matplotlib.colors import ListedColormap
    from matplotlib.patches import Patch

    pastel_cmap = ListedColormap(sns.color_palette("pastel").as_hex())
    muted_cmap = ListedColormap(sns.color_palette("muted").as_hex())
    fig, ax = plt.subplots(constrained_layout=True, figsize=(12, 6), dpi=300)

    # Get all the counts
    surface_recon_counts = count_surface_recons(df)
    data_acq = count_scans(df)
    data_proc = count_processed_scans(df)
    n_newborn_anat = data_acq.loc[("Newborn", "Anatomical")].item()
    n_newborn_dwi = data_acq.loc[("Newborn", "DWI")].item()
    n_newborn_func = data_acq.loc[("Newborn", "Functional")].item()
    n_sixmonth_anat = data_acq.loc[("Six Months", "Anatomical")].item()
    n_sixmonth_dwi = data_acq.loc[("Six Months", "DWI")].item()
    n_sixmonth_func = data_acq.loc[("Six Months", "Functional")].item()
    age_counts = {"Newborn": [n_newborn_anat, n_newborn_dwi, n_newborn_func],
                  "Six Months": [n_sixmonth_anat, n_sixmonth_dwi, n_sixmonth_func]
                  }
    
    n_newborn_anat_proc = data_proc.loc[("Newborn", "Anatomical")].item()
    n_newborn_dwi_proc = data_proc.loc[("Newborn", "DWI")].item()
    n_newborn_func_vol_proc = data_proc.loc[("Newborn", "Functional-Volume")].item()
    n_newborn_func_cifti_proc = data_proc.loc[("Newborn", "Functional-Surface")].item()
    n_sixmonth_anat_proc = data_proc.loc[("Six Months", "Anatomical")].item()
    n_sixmonth_dwi_proc = data_proc.loc[("Six Months", "DWI")].item()
    n_sixmonth_func_vol_proc = data_proc.loc[("Six Months", "Functional-Volume")].item()
    n_sixmonth_func_cifti_proc = data_proc.loc[("Six Months", "Functional-Surface")].item()

    # Set up the bar chart
    scans = ("Anatomical", "DWI", "Functional",)
    x = np.arange(len(scans))

    width = .25
    multiplier = 0

    for age, count in age_counts.items():
        offset = width * multiplier
        rects = ax.bar(x + offset, count, width, label=f"{age} Acquired", color=pastel_cmap(multiplier))
        ax.bar_label(rects, padding=3)

        if age == "Newborn":
            proc_count = [0, n_newborn_dwi_proc, n_newborn_func_vol_proc]
            surface_count = [0, 0, n_newborn_func_cifti_proc]
            mcribs_counts = surface_recon_counts.loc["Newborn", "mcribs"].item()
            mcribs_counts = [mcribs_counts, 0, 0]
            infantfs_counts = surface_recon_counts.loc["Newborn", "infantfs"].item()
            infantfs_counts = [infantfs_counts, 0, 0]
        else:
            proc_count = [0, n_sixmonth_dwi_proc, n_sixmonth_func_vol_proc]
            surface_count = [0, 0, n_sixmonth_func_cifti_proc]
            mcribs_counts = surface_recon_counts.loc["Six Months", "mcribs"].item()
            mcribs_counts = [mcribs_counts, 0, 0]
            infantfs_counts = surface_recon_counts.loc["Six Months", "infantfs"].item()
            infantfs_counts = [infantfs_counts, 0, 0]
            # Now stack the infantfs counts on top of the mcribs counts
        ax.bar(x + offset, proc_count, width, label="Processed", color=muted_cmap(multiplier))
        ax.bar(x + offset, surface_count, width, label=f"BOLD CIFTIs", color=muted_cmap(multiplier), hatch="//")
        ax.bar(x + offset, mcribs_counts, width, label="MCRIBS", color=muted_cmap(multiplier), hatch="+")
        ax.bar(x + offset, infantfs_counts, width, label="InfantFS", color=muted_cmap(multiplier), bottom=mcribs_counts)
        multiplier += 1
    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Count')
    ax.set_title("How many BABIES Scans have been Acquired and Processed?")
    ax.set_xticks(x + width, scans)
    # Let's make a custom legend.
    # 1. Pastel Blue is for Acquired Newborn
    # 2. Pastel Orange is for Acquired Six Months
    # 3. Muted Blue is for Processed Newborn
    # 4. Muted Orange is for Processed Six Months
    # 5. Transparent with + hatch is for MCRIBS
    # 6. Transparent with // hatch is for BOLD CIFTIs
    handles = [
        Patch(facecolor=pastel_cmap(0), edgecolor="black", label="Newborn Acquired"),
        Patch(facecolor=muted_cmap(0), edgecolor="black", label="Newborn Processed"),
        Patch(facecolor=pastel_cmap(1), edgecolor="black", label="Six Months Acquired"),
        Patch(facecolor=muted_cmap(1), edgecolor="black", label="Six Months Processed"),
        Patch(facecolor="white", edgecolor="black", hatch="+", label="MCRIBS"),
        Patch(facecolor="white", edgecolor="black", hatch="//", label="BOLD CIFTIs"),
    ]
    ax.legend(handles=handles, loc="upper right", title="Legend", ncols=3)

    if save:
        fig.savefig("./reports/scan_counts.png")
    return fig.show()


def parse_args():
    parser = argparse.ArgumentParser(description="Make reports for BABIES project.")
    parser.add_argument(
        "--save",
        default=True,
        choices=[True, False],
        dest="save",
        help="Save the report to disk",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    save = args.save
    df = pd.read_csv("./reports/BABIES.csv", header=[0, 1, 2], index_col=0, keep_default_na=False)
    custom_barchart_mpl(df, save=save)