import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from redcap import get_redcap_df

sns.set_theme(style="darkgrid")

SCANS = ["Anatomical", "T1w", "T2w", "Functional", "DWI"]
PROCESSED = ["Anatomical", "Functional", "DWI", "Precomputed", "Recon-all"]

# read csv files
df_newborn = pd.read_csv("./csv/participants_newborn.csv", index_col="study_id")
df_sixmonth = pd.read_csv("./csv/participants_six_month.csv", index_col="study_id")

derivatives_newborn = pd.read_csv("./csv/derivatives_newborn.csv", index_col="study_id")
derivatives_sixmonth = pd.read_csv("./csv/derivatives_six_month.csv", index_col="study_id")


#df_redcap = pd.read_csv("./csv/redcap.csv", usecols=["study_id"], dtype="string")
# Drop Duplicate Study ID's in index
#df_redcap = df_redcap.drop_duplicates("study_id")
# Drop study ID's that are not 1000-1999
#df_redcap = df_redcap[df_redcap["study_id"].str.match(r"1\d{3}")]
# Prepend "sub-" to study ID's
#df_redcap["study_id"] = "sub-" + df_redcap["study_id"]
#df_redcap.set_index("study_id", inplace=True)
df_redcap = get_redcap_df("./csv/redcap.csv", "./csv/BABIES_DataDictionary.csv")

# Now merge redcap df with newborn df
df_newborn = df_newborn.merge(df_redcap, left_index=True, right_index=True, how="outer")
df_sixmonth = df_sixmonth.merge(df_redcap, left_index=True, right_index=True, how="outer")

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


# Merge the dataframes across ages. Set another index as "visit"
df_acquired = pd.concat(
    [df_newborn, df_sixmonth],
    axis=1,
    keys=["Newborn", "Six Months"],
    names=["Visit", "Scan"],
)

# Set Multi-index
# df_acquired.columns = df_acquired.columns.set_levels(["Visit"], level=0)
# df_acquired.columns = df_acquired.columns.set_levels(["Visit", "Scan"], level=1)
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

# report = df_nibabies.T.groupby(level=[0, 1]).sum().sum(axis=1)

# report_babies = df_babies.T.groupby(level=[0, 1, 2]).sum().sum(axis=1)
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
                else:
                    pass
        elif all([row[this_scan] == True for this_scan in these_scans]):
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

to_drop_newborn = [("Acquired", "Newborn", col) for col in ["sixmo_status_v2", "sixmo_notscan_v2"]]
to_drop_sixmonth = [("Acquired", "Six Months", col) for col in ["neonatal_status_v2", "neonatal_notscan_v2"]]
df_babies = df_babies.drop(columns=to_drop_newborn + to_drop_sixmonth)
to_drop = [("Acquired", age, col) for age in ["Newborn", "Six Months"] for col in ["infant_sex", "child_sex"]]
df_babies = df_babies.drop(columns=to_drop + [("Acquired", "Six Months", "Biological Sex")])

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
assert 1 == 0

# Save DataFrames to csv
df_acquired.to_csv(
    "./reports/acquired.csv"
)  # pd.read_csv(..., header=[0, 1], index_col=[0])
df_nibabies.to_csv("./reports/nibabies.csv")
df_babies.to_csv(
    "./reports/BABIES.csv"
)  # pd.read_csv(..., header=[0, 1, 2], index_col=[0])

### Let's nicely format the data for Google sheets ####
### Within a session, if some but not all scans are missing, we should mark the missing scans as "Not Acquired"
### if all scans are there, we should mark each scans as "Acquired"

# first some missing:
for ii, row in df_babies.T.iterrows():
    for age in ["Newborn", "Six Months"]:
        these_scans = [row[age, col] for col in SCANS]
        if any([is_falsey(row[these_scans]) for col in these_scans]):
            for col in these_scans:
                df_babies.at[ii, col] = "Not Acquired"

# Create plots
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="darkgrid")

fig, ax = plt.subplots(figsize=(10, 5), constrained_layout=True)

# Grouped bar chart of Scan counts, X axis Scan type, Hue by visit,
to_drop = [("Acquired", visit, scan) for visit in ["Newborn", "Six Months"] for scan in ["T1w", "T2w"]]
data = df_babies.T.drop("Processed").drop(to_drop)
data = data.groupby(level=[0, 1, 2]).sum().sum(axis=1).to_frame(name="Value").stack().reset_index().rename(columns={0: "Value"})

with sns.color_palette("pastel"):
    sns.barplot(x="Scan", y="Value", hue="Visit", data=data, ax=ax)

data.to_csv("./reports/scan_counts.csv")

to_drop = [("Processed", visit, col) for visit in ["Newborn", "Six Months"] for col in ["Surface-Recon-Method", "Date-Processed", "Precomputed", "Recon-all"]]
data = df_babies.T.drop("Acquired").drop(to_drop).groupby(by=["Visit", "Scan"]).sum().sum(axis=1)
data = data.to_frame().stack().reset_index().rename(columns={0: "Value"})
with sns.color_palette("muted"):
    sns.barplot(
        x="Scan",
        y="Value", 
        hue="Visit", 
        data=data, 
        ax=ax,
        linewidth=2.5,
        edgecolor=".5",
        )
data.to_csv("./reports/processed_counts.csv")
fig.savefig("./reports/scan_counts.png")

#cond_new = ((df_babies[("Acquired", "Newborn", "Anatomical")] == True
# cond_six = ((df_babies[("Acquired", "Six Months", "Anatomical")] == True) |
