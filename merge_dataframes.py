import argparse
from warnings import warn

import numpy as np
import pandas as pd
from paths import get_csv_paths
from redcap import get_redcap_df

SCANS = ["Anatomical", "T1w", "T2w", "Functional", "DWI"]
PROCESSED = ["Anatomical", "Functional-Volume", "Functional-Surface", "DWI", "Precomputed", "Recon-all"]


def is_falsey(x):
    return not x or pd.isna(x)


def _get_missing_reason(row, age):
    """Try to get the missing reason for a scan from the notscan column, fallback to status column"""
    age_map = dict(newborn="neonatal", sixmonth="sixmo")
    age = age_map[age]
    if is_falsey(row[f"{age}_notscan_v2"]):
        if is_falsey(row[f"{age}_status_v2"]) or row[f"{age}_status_v2"] == "Completed":
            return "No MRI"
        return row[f"{age}_status_v2"]
    return row[f"{age}_notscan_v2"]



def refine_the_dataframe(df_babies):
    for ii, row in df_babies.iterrows():
        for age in ["Newborn", "Six Months"]:
            these_scans = [("Acquired", age, col) for col in SCANS]
            if all([is_falsey(row[this_scan]) or isinstance(row[this_scan], str) for this_scan in these_scans]):
                if age == "Newborn":
                    if row[("Acquired", "Newborn", "neonatal_status_v2")] == False:
                        df_babies.at[ii, ("Acquired", "Newborn", "neonatal_status_v2")] = "Unknown"
                    if row[("Acquired", "Newborn", "neonatal_notscan_v2")] == False:
                        df_babies.at[ii, ("Acquired", "Newborn", "neonatal_notscan_v2")] = "Unknown"
                elif age == "Six Months":
                    if row[("Acquired", "Six Months", "sixmo_status_v2")] == False:
                        df_babies.at[ii, ("Acquired", "Six Months", "sixmo_status_v2")] = "Unknown"
                    if row[("Acquired", "Six Months", "sixmo_notscan_v2")] == False:
                        df_babies.at[ii, ("Acquired", "Six Months", "sixmo_notscan_v2")] = "Unknown"
                if all([is_falsey(row[this_scan]) for this_scan in these_scans]):
                    for col in these_scans:
                        df_babies.at[ii, col] = "Not Acquired"
                for col in row[("Processed", age)].index:
                    df_babies.at[ii, ("Processed", age, col)] = "N/A"
                    df_babies.at[ii, ("Processed", age, "Surface-Recon-Method")] = "N/A"
                    df_babies.at[ii, ("Processed", age, "Date-Processed")] = "N/A"
            elif any([is_falsey(row[this_scan]) or isinstance(row[this_scan], str) for this_scan in these_scans]):
                if age == "Newborn":
                    if row[("Acquired", "Newborn", "neonatal_status_v2")] == False:
                        df_babies.at[ii, ("Acquired", "Newborn", "neonatal_status_v2")] = "Unknown"
                    if row[("Acquired", "Newborn", "neonatal_notscan_v2")] == False:
                        df_babies.at[ii, ("Acquired", "Newborn", "neonatal_notscan_v2")] = "Unknown"
                elif age == "Six Months":
                    if row[("Acquired", "Six Months", "sixmo_status_v2")] == False:
                        df_babies.at[ii, ("Acquired", "Six Months", "sixmo_status_v2")] = "Unknown"
                    if row[("Acquired", "Six Months", "sixmo_notscan_v2")] == False:
                        df_babies.at[ii, ("Acquired", "Six Months", "sixmo_notscan_v2")] = "Unknown"
                for col in these_scans:
                    scan = col[-1]
                    # if the scan is not acquired, set the corresponding processed columns to "N/A"
                    if is_falsey(row[col]) or isinstance(row[col], str):
                        if is_falsey(row[col]):
                            df_babies.at[ii, col] = "Not Acquired"
                        try:
                            row[("Processed", age, scan)]
                            df_babies.at[ii, ("Processed", age, scan)] = "N/A"
                        except KeyError:
                            pass
                        if scan == "Anatomical":
                            df_babies.at[ii, ("Processed", age, "Surface-Recon-Method")] = "N/A"
                            df_babies.at[ii, ("Processed", age, "Date-Processed")] = "N/A"
                            df_babies.at[ii, ("Processed", age, "Precomputed")] = "N/A"
                            df_babies.at[ii, ("Processed", age, "Recon-all")] = "N/A"
                        elif scan == "Functional":
                            df_babies.at[ii, ("Processed", age, "Functional-Volume")] = "N/A"
                            df_babies.at[ii, ("Processed", age, "Functional-Surface")] = "N/A"
                    elif row[col] == True:
                        df_babies.at[ii, col] = "Acquired"
                        try:
                            row[("Processed", age, scan)]
                            if is_falsey(row[("Processed", age, scan)]):
                                df_babies.at[ii, ("Processed", age, scan)] = "Not Processed"
                                if scan == "Anatomical":
                                    df_babies.at[ii, ("Processed", age, "Surface-Recon-Method")] = "Not Processed"
                                    df_babies.at[ii, ("Processed", age, "Date-Processed")] = "Not Processed"
                            elif row[("Processed", age, scan)] == True:
                                df_babies.at[ii, ("Processed", age, scan)] = "Processed"
                        except KeyError:
                            pass
                        if scan == "Anatomical":
                            if is_falsey(row[("Processed", age, "Precomputed")]):
                                df_babies.at[ii, ("Processed", age, "Precomputed")] = "Not Processed"
                            elif row[("Processed", age, "Precomputed")] == True:
                                df_babies.at[ii, ("Processed", age, "Precomputed")] = "Processed"
                            if is_falsey(row[("Processed", age, "Recon-all")]):
                                df_babies.at[ii, ("Processed", age, "Recon-all")] = "Not Processed"
                            elif row[("Processed", age, "Recon-all")] == True:
                                df_babies.at[ii, ("Processed", age, "Recon-all")] = "Processed"
                        elif scan == "Functional":
                            if is_falsey(row[("Processed", age, "Functional-Volume")]):
                                df_babies.at[ii, ("Processed", age, "Functional-Volume")] = "Not Processed"
                            elif row[("Processed", age, "Functional-Volume")] == True:
                                df_babies.at[ii, ("Processed", age, "Functional-Volume")] = "Processed"
                            if is_falsey(row[("Processed", age, "Functional-Surface")]):
                                df_babies.at[ii, ("Processed", age, "Functional-Surface")] = "Not Processed"
                            elif row[("Processed", age, "Functional-Surface")] == True:
                                df_babies.at[ii, ("Processed", age, "Functional-Surface")] = "Processed"
                    else:
                        pass
            elif all([row[this_scan] == True for this_scan in these_scans]):
                # if all scans are acquired, set the status to "Completed"
                # and the reason not acquired to "N/A"
                if age == "Newborn":
                    df_babies.at[ii, ("Acquired", "Newborn", "neonatal_status_v2")] = "Completed"
                    df_babies.at[ii, ("Acquired", "Newborn", "neonatal_notscan_v2")] = "N/A"
                elif age == "Six Months":
                    df_babies.at[ii, ("Acquired", "Six Months", "sixmo_status_v2")] = "Completed"
                    df_babies.at[ii, ("Acquired", "Six Months", "sixmo_notscan_v2")] = "N/A"
                for col in these_scans:
                    scan = col[-1]
                    df_babies.at[ii, col] = "Acquired"
                    try:
                        row[("Processed", age, scan)]
                        if is_falsey(row[("Processed", age, scan)]):
                            df_babies.at[ii, ("Processed", age, scan)] = "Not Processed"
                        elif row[("Processed", age, scan)] == True:
                            df_babies.at[ii, ("Processed", age, scan)] = "Processed"
                        else:
                            pass
                    except KeyError:
                        pass
                    if scan == "Anatomical":
                        if is_falsey(row[("Processed", age, "Precomputed")]):
                            df_babies.at[ii, ("Processed", age, "Precomputed")] = "Not Processed"
                        elif row[("Processed", age, "Precomputed")] == True:
                            df_babies.at[ii, ("Processed", age, "Precomputed")] = "Processed"
                        if is_falsey(row[("Processed", age, "Recon-all")]):
                            df_babies.at[ii, ("Processed", age, "Recon-all")] = "Not Processed"
                        elif row[("Processed", age, "Recon-all")] == True:
                            df_babies.at[ii, ("Processed", age, "Recon-all")] = "Processed"
                    elif scan == "Functional":
                        if is_falsey(row[("Processed", age, "Functional-Volume")]):
                            df_babies.at[ii, ("Processed", age, "Functional-Volume")] = "Not Processed"
                        elif row[("Processed", age, "Functional-Volume")] == True:
                            df_babies.at[ii, ("Processed", age, "Functional-Volume")] = "Processed"
                        if is_falsey(row[("Processed", age, "Functional-Surface")]):
                            df_babies.at[ii, ("Processed", age, "Functional-Surface")] = "Not Processed"
                        elif row[("Processed", age, "Functional-Surface")] == True:
                            df_babies.at[ii, ("Processed", age, "Functional-Surface")] = "Processed"

    to_drop_newborn = [("Acquired", "Newborn", col) for col in ["sixmo_status_v2", "sixmo_notscan_v2"]]
    to_drop_sixmonth = [("Acquired", "Six Months", col) for col in ["neonatal_status_v2", "neonatal_notscan_v2"]]
    df_babies = df_babies.drop(columns=to_drop_newborn + to_drop_sixmonth)
    # Don't need two biological sex columns
    df_babies = df_babies.drop(columns=[("Acquired", "Six Months", "Biological Sex")])
    # If any of the Newborn Biolobical Sex Values are False, just set to "Missing"
    for ii, row in df_babies.iterrows():
        if row[("Acquired", "Newborn", "Biological Sex")] == False:
            df_babies.at[ii, ("Acquired", "Newborn", "Biological Sex")] = "Missing"

    df_babies.rename(
        columns={"neonatal_status_v2": "Status",
                "neonatal_notscan_v2": "Reason Not-Acquired",
                "sixmo_status_v2": "Status",
                "sixmo_notscan_v2": "Reason Not-Acquired"},
        inplace=True
        )

    reordered_columns = [("Acquired", "Newborn", "Biological Sex")] + [col for col in df_babies.columns if col != ("Acquired", "Newborn", "Biological Sex")]
    df_babies = df_babies[reordered_columns]
    # now format the processed columns:
    # 1. If the scan is not acquired, set the corresponding processed columns to "N/A"
    # 2. If the scan is acquired, set the corresponding processed columns to "Not Processed" if they are False
    # 3. If the scan is acquired, set the corresponding processed columns to "Processed" if they are True
    return df_babies

def build_dataframe(project):
    idx_col = "study_id"
    csvs = get_csv_paths(project)
    df_newborn = pd.read_csv(csvs[f"acquisition_newborn"], index_col=idx_col)
    df_sixmonth = pd.read_csv(csvs[f"acquisition_sixmonth"], index_col=idx_col)
    derivatives_newborn = pd.read_csv(csvs[f"derivatives_newborn"], index_col=idx_col)
    derivatives_sixmonth = pd.read_csv(csvs[f"derivatives_sixmonth"], index_col=idx_col)
    df_redcap = get_redcap_df(csvs["redcap"], csvs["datadict"], project)
    # Now merge redcap df with newborn df
    df_newborn = df_newborn.merge(df_redcap, left_index=True, right_index=True, how="outer")
    df_sixmonth = df_sixmonth.merge(df_redcap, left_index=True, right_index=True, how="outer")
    # Merge the dataframes across ages. Set another index as "visit"
    df_acquired = pd.concat(
        [df_newborn, df_sixmonth],
        axis=1,
        keys=["Newborn", "Six Months"],
        names=["Visit", "Scan"],
    )
    # Set Multi-index
    df_nibabies = pd.concat(
        [derivatives_newborn, derivatives_sixmonth],
        axis=1,
        keys=["Newborn", "Six Months"],
        names=["Visit", "Scan"],
    )  
    with pd.option_context("future.no_silent_downcasting", True):
        df_nibabies = df_nibabies.fillna(False)
    df_babies = pd.concat(
        [df_acquired, df_nibabies],
        axis=1,
        keys=["Acquired", "Processed"],
        names=["Stage", "Visit", "Scan"],
    )
    with pd.option_context("future.no_silent_downcasting", True):
        df_babies = df_babies.fillna(False)
    # Make the dataframe more readable
    df_babies = refine_the_dataframe(df_babies)
    # assert that there are no np.nans in the dataframe
    if df_babies.isnull().values.any():
        # Where are the np.nans?
        n_nans = df_babies.isnull().sum()
        warn(f"There are {n_nans} np.nans in the dataframe. Please check the dataframe.")
    # check if any False values are present in the dataframe
    if df_babies.isin([False]).values.any():
        # How many False values are there?
        n_false = df_babies.isin([False]).sum()
        warn(f"There are False values in the dataframe. Please check the dataframe.")

    # Save
    # Save DataFrames to csv
    df_babies.to_csv(f"./reports/{project}_final.csv")


def parse_args():
    parser = argparse.ArgumentParser(description="Build the final dataframe for project tracking.")
    parser.add_argument("--project",
                        type=str,
                        required=True,
                        choices=["ABC", "BABIES",],
                        dest="project",
                        help="Project name. Must be 'ABC' or 'BABIES'.",
                        )
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    project = args.project
    build_dataframe(project)
