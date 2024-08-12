import argparse

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set_theme(style="darkgrid")


def count_scans(df, project):
    # Grouped bar chart of Scan counts, X axis Scan type, Hue by visit,
    drop_these_cols = ["T1w", "T2w", "Status", "Reason Not-Acquired"]
    visits = ["Newborn", "Six Months"]
    # ABC
    if project == "ABC":
        visits.append("Twelve Months")
    to_drop = [("Acquired", visit, scan) for visit in visits for scan in drop_these_cols]
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

def count_all_scans(df, project, save=True):
    scan_counts = count_scans(df, project)
    proc_counts = count_processed_scans(df, project)
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
    # ABC
    if project == "ABC":
        want_indices_twelvemonth = [("Twelve Months", scan) for scan in ["Functional-Volume", "Functional-Surface"]]
        functional_counts_twelvemonth = all_counts.loc[want_indices_twelvemonth, want_cols].sum()
        all_counts.at[("Twelve Months", "Functional"), "Processed"] = functional_counts_twelvemonth
    if save:
        all_counts.to_csv(f"./reports/{project}_all_scan_counts.csv")
    return all_counts


def count_processed_scans(df, project):
    drop_these_cols = ["Surface-Recon-Method", "Date-Processed", "Precomputed", "Recon-all"]
    visits = ["Newborn", "Six Months"]
    # ABC
    if project == "ABC":
        visits.append("Twelve Months")
    to_drop = [("Processed", visit, col)
               for visit in visits
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

def count_surface_recons(df, project):
    visits = ["Newborn", "Six Months"]
    # ABC
    if project == "ABC":
        visits.append("Twelve Months")
    want_cols = [("Processed", age, "Surface-Recon-Method") for age in visits]
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

def custom_barchart_mpl(df, project, save=True):
    import numpy as np
    from matplotlib.colors import ListedColormap
    from matplotlib.patches import Patch

    assert project in ["ABC", "BABIES"]

    pastel_cmap = ListedColormap(sns.color_palette("pastel").as_hex())
    muted_cmap = ListedColormap(sns.color_palette("muted").as_hex())
    fig, ax = plt.subplots(constrained_layout=True, figsize=(12, 6), dpi=300)

    # Get all the counts
    surface_recon_counts = count_surface_recons(df, project=project)
    data_acq = count_scans(df, project=project)
    data_proc = count_processed_scans(df, project=project)
    n_newborn_anat = data_acq.loc[("Newborn", "Anatomical")].item()
    n_newborn_dwi = data_acq.loc[("Newborn", "DWI")].item()
    n_newborn_func = data_acq.loc[("Newborn", "Functional")].item()
    n_sixmonth_anat = data_acq.loc[("Six Months", "Anatomical")].item()
    n_sixmonth_dwi = data_acq.loc[("Six Months", "DWI")].item()
    n_sixmonth_func = data_acq.loc[("Six Months", "Functional")].item()

    # ABC
    if project == "ABC":
        n_twelvemonth_anat = data_acq.loc[("Twelve Months", "Anatomical")].item()
        n_twelvemonth_dwi = data_acq.loc[("Twelve Months", "DWI")].item()
        n_twelvemonth_func = data_acq.loc[("Twelve Months", "Functional")].item()

    age_counts = {"Newborn": [n_newborn_anat, n_newborn_dwi, n_newborn_func],
                  "Six Months": [n_sixmonth_anat, n_sixmonth_dwi, n_sixmonth_func],
                  }
    # ABC
    if project == "ABC":
        age_counts["Twelve Months"] = [n_twelvemonth_anat, n_twelvemonth_dwi, n_twelvemonth_func]
    
    n_newborn_anat_proc = data_proc.loc[("Newborn", "Anatomical")].item()
    n_newborn_dwi_proc = data_proc.loc[("Newborn", "DWI")].item()
    n_newborn_func_vol_proc = data_proc.loc[("Newborn", "Functional-Volume")].item()
    n_newborn_func_cifti_proc = data_proc.loc[("Newborn", "Functional-Surface")].item()
    n_sixmonth_anat_proc = data_proc.loc[("Six Months", "Anatomical")].item()
    n_sixmonth_dwi_proc = data_proc.loc[("Six Months", "DWI")].item()
    n_sixmonth_func_vol_proc = data_proc.loc[("Six Months", "Functional-Volume")].item()
    n_sixmonth_func_cifti_proc = data_proc.loc[("Six Months", "Functional-Surface")].item()

    # ABC
    if project == "ABC":
        n_twelvemonth_anat_proc = data_proc.loc[("Twelve Months", "Anatomical")].item()
        n_twelvemonth_dwi_proc = data_proc.loc[("Twelve Months", "DWI")].item()
        n_twelvemonth_func_vol_proc = data_proc.loc[("Twelve Months", "Functional-Volume")].item()
        n_twelvemonth_func_cifti_proc = data_proc.loc[("Twelve Months", "Functional-Surface")].item()

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
            proc_count = [n_newborn_anat_proc, n_newborn_dwi_proc, n_newborn_func_vol_proc]
            cifti_count = [0, 0, n_newborn_func_cifti_proc]
            subcortical_count = [0, 0, n_newborn_func_vol_proc]
            mcribs_counts = surface_recon_counts.loc["Newborn", "mcribs"].item()
            mcribs_counts = [mcribs_counts, 0, 0]
            infantfs_counts = surface_recon_counts.loc["Newborn", "infantfs"].item()
            infantfs_counts = [infantfs_counts, 0, 0]
        elif age == "Six Months":
            proc_count = [n_sixmonth_anat_proc, n_sixmonth_dwi_proc, n_sixmonth_func_vol_proc]
            cifti_count = [0, 0, n_sixmonth_func_cifti_proc]
            subcortical_count = [0, 0, n_sixmonth_func_vol_proc]
            mcribs_counts = surface_recon_counts.loc["Six Months", "mcribs"].item()
            mcribs_counts = [mcribs_counts, 0, 0]
            infantfs_counts = surface_recon_counts.loc["Six Months", "infantfs"].item()
            infantfs_counts = [infantfs_counts, 0, 0]
        # ABC
        elif age == "Twelve Months":
            proc_count = [n_twelvemonth_anat_proc, n_twelvemonth_dwi_proc, n_twelvemonth_func_vol_proc]
            cifti_count = [0, 0, n_twelvemonth_func_cifti_proc]
            subcortical_count = [0, 0, n_twelvemonth_func_vol_proc]
            mcribs_counts = surface_recon_counts.loc["Twelve Months", "mcribs"].item()
            mcribs_counts = [mcribs_counts, 0, 0]
            infantfs_counts = surface_recon_counts.loc["Twelve Months", "infantfs"].item()
            infantfs_counts = [infantfs_counts, 0, 0]
        else:
            raise ValueError(f"Age {age} is not recognized.")
        # Now stack the infantfs counts on top of the mcribs counts
        ax.bar(x + offset, proc_count, width, label="Processed", color=muted_cmap(multiplier))
        ax.bar(x + offset, cifti_count, width, label=f"Functional Cortical", color=muted_cmap(multiplier), hatch="//")
        ax.bar(x + offset, mcribs_counts, width, label="MCRIBS", color=muted_cmap(multiplier), hatch="+")
        multiplier += 1
    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Count')
    ax.set_title(f"How many {project} Scans have been Acquired and Processed?")
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
    ]
    # ABC
    if project == "ABC":
        handles += [
            Patch(facecolor=pastel_cmap(2), edgecolor="black", label="Twelve Months Acquired"),
            Patch(facecolor=muted_cmap(2), edgecolor="black", label="Twelve Months Processed"),
        ]
    handles += [
        Patch(facecolor="white", edgecolor="black", hatch="+", label="MCRIBS"),
        Patch(facecolor="white", edgecolor="black", hatch="//", label="Functional Cortical"),
    ]
    ncols = 4 if project == "ABC" else 3
    ax.legend(handles=handles, loc="upper right", title="Legend", ncols=ncols)

    if save:
        fig.savefig(f"./reports/{project}_scan_counts.png")
    return fig.show()


def parse_args():
    parser = argparse.ArgumentParser(description="Make reports for BABIES or ABC project.")
    parser.add_argument(
        "--project",
        type=str,
        required=True,
        choices=["ABC", "BABIES"],
        dest="project",
        help="Project name. Must be 'ABC' or 'BABIES'.",
    )
    parser.add_argument(
        "--save",
        default=True,
        choices=[True, False],
        dest="save",
        help="Save the report to disk",
    )
    parser.add_argument(
        "--save-counts",
        action="store_true",
        dest="save_counts",
        help="Save the counts to a CSV file",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    project = args.project
    save = args.save
    save_counts = vars(args).get("save_counts", False)
    df = pd.read_csv(f"./reports/{project}_final.csv", header=[0, 1, 2], index_col=0, keep_default_na=False)
    custom_barchart_mpl(df, project=project, save=save)
    if save_counts:
        count_all_scans(df, project=project, save=True)