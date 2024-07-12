import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set_theme(style="darkgrid")

# read csv files
df_newborn = pd.read_csv("./csv/participants_newborn.csv", index_col="study_id")
df_sixmonth = pd.read_csv("participants_six_month.csv", index_col="study_id")
derivatives_newborn = pd.read_csv("derivative_newborn.csv", index_col="study_id")
derivatives_sixmonth = pd.read_csv("derivative_six_month.csv", index_col="study_id")
df_redcap = pd.read_csv("redcap.csv", usecols=["study_id"], dtype="string")
# Drop Duplicate Study ID's in index
df_redcap = df_redcap.drop_duplicates("study_id")
# Drop study ID's that are not 1000-1999
df_redcap = df_redcap[df_redcap["study_id"].str.match(r"1\d{3}")]
# Prepend "sub-" to study ID's
df_redcap["study_id"] = "sub-" + df_redcap["study_id"]
df_redcap.set_index("study_id", inplace=True)

# Now merge redcap df with newborn df
df_newborn = df_newborn.merge(df_redcap, left_index=True, right_index=True, how="outer")


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
    [nibabies_newborn, nibabies_sixmonth],
    axis=1,
    keys=["Newborn", "Six Months"],
    names=["Visit", "Scan"],
)
df_nibabies = df_nibabies.fillna(False)

df_babies = pd.concat(
    [df_acquired, df_nibabies],
    axis=1,
    keys=["Acquired", "NiBabies"],
    names=["Stage", "Visit", "Scan"],
)

df_babies = df_babies.fillna(False)

report = df_nibabies.T.groupby(level=[0, 1]).sum().sum(axis=1)

report_babies = df_babies.T.groupby(level=[0, 1, 2]).sum().sum(axis=1)

# Save DataFrames to csv
df_acquired.to_csv(
    "./reports/acquired.csv"
)  # pd.read_csv(..., header=[0, 1], index_col=[0])
df_nibabies.to_csv("./reports/nibabies.csv")
df_babies.to_csv(
    "./reports/BABIES.csv"
)  # pd.read_csv(..., header=[0, 1, 2], index_col=[0])


# Create plots
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="darkgrid")

fig, ax = plt.subplots(figsize=(10, 5), constrained_layout=True)

data = df_acquired.fillna(False).groupby(level=[0]).sum().sum(axis=0)
report = df_acquired.fillna(False).groupby(level=[0]).sum().sum(axis=0).plot(kind="bar", ax=ax)

# Grouped bar chart of Scan counts, X axis Scan type, Hue by visit,
to_drop = [("Acquired", visit, scan) for visit in ["Newborn", "Six Months"] for scan in ["T1w", "T2w"]]
data = df_babies.T.drop("NiBabies").drop(to_drop)
data = data.groupby(level=[0, 1, 2]).sum().sum(axis=1).to_frame(name="Value").stack().reset_index().rename(columns={0: "Value"})

with sns.color_palette("pastel"):
    sns.barplot(x="Scan", y="Value", hue="Visit", data=data, ax=ax)

data = df_babies.T.drop("Acquired").groupby(by=["Visit", "Scan"]).sum().sum(axis=1)
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

