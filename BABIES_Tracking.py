import argparse
import re
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd
import toml

# setup Argparse
# Define the possible choices for SESSION
SESSION_CHOICES = ['newborn', 'six_month']

# Parse command line arguments
parser = argparse.ArgumentParser(description="Process MRI data.")
parser.add_argument("session", choices=SESSION_CHOICES, help="MRI session (newborn or six_month)")
args = parser.parse_args()

# Assign SESSION based on the command line argument
SESSION = args.session

# Constants
SERVER_PATH = Path("/Volumes") / "HumphreysLab" / "Daily_2" / "BABIES" / "MRI" / SESSION
BIDS_PATH = SERVER_PATH / "bids"
DERIVATIVES_PATH = SERVER_PATH / "derivatives"
SI_PATH = SERVER_PATH.parent / "SI_data" / "derivatives"/ "nibabies_new"

# Script
def build_acquisition_csv(session):

    print("👇 Documenting which participants received MRI scans 👇")
    # Extract the sub-* foldernames and write to file for later
    command = f"ls -d {BIDS_PATH / 'sub-*'} | xargs -n1 basename"

    output = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, text=True)

    csv_fname = Path(f"./csv/participants_{session}.csv")
    with csv_fname.open("w") as f:
        f.write(output.stdout)

    df = pd.read_csv(csv_fname, header=None, names=["study_id"])

    df[f"Anatomical"] = None
    df[f"T1w"] = None
    df[f"T2w"] = None
    df[f"Functional"] = None
    df[f"DWI"] = None
    for i, series in df.iterrows():
        sub = series["study_id"]
        sub_path = BIDS_PATH / sub
        ses = "sixmonth" if session == "six_month" else "newborn"
        anat_path = sub_path / f"ses-{ses}" / "anat"
        func_path = sub_path / f"ses-{ses}" / "func"
        dwi_path = sub_path / f"ses-{ses}" / "dwi"

        has_t1w = any(anat_path.glob("*_T1w.*"))
        has_t2w = any(anat_path.glob("*_T2w.*"))
        if not has_t1w and not has_t2w:
            anat_raw_path = sub_path / f"ses-{ses}" / "anat_raw"
            if anat_raw_path.exists():
                has_t1w = any(anat_raw_path.glob("*_T1w.*"))
                has_t2w = any(anat_raw_path.glob("*_T2w.*"))

        df.loc[i, f"T1w"] = has_t1w
        df.loc[i, f"T2w"] = has_t2w
        df.loc[i, f"Anatomical"] = has_t1w or has_t2w

        has_func = any(func_path.glob("*_bold.*"))
        df.loc[i, f"Functional"] = has_func

        has_dwi = any(dwi_path.glob("*_dwi.*"))
        df.loc[i, f"DWI"] = has_dwi
        print(".", end="", flush=True)

    # Save file
    print(f"Saving CSV file to {csv_fname.resolve()}")
    df.to_csv(csv_fname, index=False)

def build_derivatives_csv(session):
    """ Build a CSV File for Nibabies, precomputed, and other derivatives."""
    # Extract the sub-* foldernames and write to file for later
    # Nibabies
    nibabies_df = build_nibabies_csv(session)
    dwi_df = build_dwi_csv(session)
    precomputed_df = build_precomputed_df(session)
    reconall_df = build_reconall_df(session)
    # Merge the dataframes
    df = nibabies_df.merge(dwi_df, on="study_id", how="outer")
    df = df.merge(precomputed_df, on="study_id", how="outer")
    df = df.merge(reconall_df, on="study_id", how="outer")
    # Save file
    csv_fname = Path(f"./csv/derivatives_{session}.csv")    
    print(f"Saving Derivatives CSV file to {csv_fname.resolve()}")
    df.to_csv(csv_fname, index=False)
    return df

def build_nibabies_csv(session):
    """ Build a CSV File for Nibabies derivatives."""
    print("👇 Documenting which subjects were processed with Nibabies! 👇")
    nibabies_path = DERIVATIVES_PATH / "Nibabies"
    command = f"ls -d {nibabies_path / 'sub-*'} | grep -v '\.html$' | xargs -n1 basename"
    output = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, text=True)
    csv_fname = Path(f"./csv/nibabies_{session}.csv")
    with csv_fname.open("w") as f:
        f.write(output.stdout)
    
    df = pd.read_csv(csv_fname, header=None, names=["study_id"])
    df["Anatomical"] = None
    df["Functional"] = None
    df["Surface-Recon-Method"] = None

    SI_df = build_SI_data_df(session)
    for i, series in df.iterrows():
        sub = series["study_id"]
        sub_path = nibabies_path / sub
        ses = "sixmonth" if session == "six_month" else "newborn"
        ses_path = sub_path / f"ses-{ses}"
        anat_path = ses_path / "anat"
        func_path = ses_path / "func"
        # Have to load the toml file from the log folder
        # We use the most recent run to specify the surface recon method
        log_path = ses_path / "log"
        toml_data = load_nibabies_toml(log_path)

        has_anat = anat_path.exists() and any(anat_path.glob("*"))
        df.loc[i, f"Anatomical"] = has_anat

        has_func = func_path.exists() and any(func_path.glob("*"))
        has_volume = func_path.exists() and any(func_path.glob("*_boldref.nii.gz"))
        has_cifti = func_path.exists() and any(func_path.glob("*k_boldref.dtseries.nii*"))

        # check if subject in SI_data

        exists_in_SI = sub in SI_df["study_id"].values
        has_SI_volume = exists_in_SI and SI_df.loc[SI_df["study_id"] == sub, "Volume"].values[0]
        has_SI_cifti = exists_in_SI and SI_df.loc[SI_df["study_id"] == sub, "Cifti"].values[0]

        df.loc[i, f"Functional"] = has_func
        df.loc[i, f"Functional-Volume"] = has_volume or has_SI_volume
        df.loc[i, f"Functional-Surface"] = has_cifti or has_SI_cifti

        # Check for surface recon method
        recon_method = toml_data["workflow"]["surface_recon_method"]
        df.loc[i, f"Surface-Recon-Method"] = recon_method

        # Extract the processing date
        processing_date = extract_processing_datetime(log_path)
        df.loc[i, "Date-Processed"] = processing_date
        print(".", end="", flush=True)
    # Save file
    print(f"\n Saving Nibabies CSV file to {csv_fname.resolve()}")
    df.to_csv(csv_fname, index=False)
    print("✅ Done!")
    return df

def build_SI_data_df(session):
    """ Build a CSV File for SI data."""
    command = f"ls -d {SI_PATH / 'sub-*/'} | xargs -n1 basename"
    output = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, text=True)
    csv_fname = Path(f"./csv/SI_data_{session}.csv")
    with csv_fname.open("w") as f:
        f.write(output.stdout)
    df = pd.read_csv(csv_fname, header=None, names=["study_id"])
    df[f"SI_data"] = None
    df[f"Volume"] = None
    df[f"Cifti"] = None
    for i, series in df.iterrows():
        sub = series["study_id"]
        sub_path = SI_data_path / sub
        func_path = sub_path / f"ses-{session}" / "func"
        has_SI_data = func_path.exists() and any(sub_path.glob("*"))
        df.loc[i, f"SI_data"] = has_SI_data
        has_volume = func_path.exists() and any(func_path.glob("*_boldref.nii.gz"))
        has_cifti = func_path.exists() and any(func_path.glob("*k_boldref.dtseries.nii*"))
        df.loc[i, f"Volume"] = has_volume
        df.loc[i, f"Cifti"] = has_cifti

    return df

def build_dwi_csv(session):
    """ Build a CSV File for DWI derivatives."""
    # Extract the sub-* foldernames and write to file for later
    # Nibabies
    print("👇 Documenting which subjects Have processed DWI data! 👇")
    dwi_path = DERIVATIVES_PATH / "Diffusion"
    command = f"ls -d {dwi_path / 'sub-*'} | xargs -n1 basename"
    output = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, text=True)
    csv_fname = Path(f"./csv/dwi_{session}.csv")
    with csv_fname.open("w") as f:
        f.write(output.stdout)
    df = pd.read_csv(csv_fname, header=None, names=["study_id"])
    df[f"DWI"] = None
    for i, series in df.iterrows():
        sub = series["study_id"]
        sub_path = dwi_path / sub
        ses = "sixmonth" if session == "six_month" else "newborn"

        has_dwi = sub_path.exists() and any(sub_path.glob("*"))
        df.loc[i, f"DWI"] = has_dwi
        print(".", end="", flush=True)
    print(f"\n Saving DWI CSV file to {csv_fname.resolve()}")
    return df

def build_precomputed_df(session):
    """ Build a CSV File for Precomputed derivatives."""
    print("👇 Documenting which subjects Have a Manually Segmented Scan! 👇")
    precomputed_path = DERIVATIVES_PATH / "precomputed"
    command = f"ls -d {precomputed_path / 'sub-*'} | xargs -n1 basename"
    output = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, text=True)
    csv_fname = Path(f"./csv/precomputed_{session}.csv")
    with csv_fname.open("w") as f:
        f.write(output.stdout)
    df = pd.read_csv(csv_fname, header=None, names=["study_id"])
    df[f"Precomputed"] = None
    for i, series in df.iterrows():
        sub = series["study_id"]
        sub_path = precomputed_path / sub
        has_precomputed = sub_path.exists() and any(sub_path.glob("*"))
        df.loc[i, f"Precomputed"] = has_precomputed
        print(".", end="", flush=True)
    print(f"\n Saving Precomputed CSV file to {csv_fname.resolve()}")
    return df

def build_reconall_df(session):
    """ Build a CSV File for Recon-All derivatives."""
    print("👇 Documenting which subjects Have a Recon-All Derivative! 👇")
    reconall_path = DERIVATIVES_PATH / "recon-all"
    command = f"ls -d {reconall_path / 'sub-*'} | xargs -n1 basename"
    output = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, text=True)
    csv_fname = Path(f"./csv/reconall_{session}.csv")
    with csv_fname.open("w") as f:
        f.write(output.stdout)
    df = pd.read_csv(csv_fname, header=None, names=["study_id"])
    df[f"Recon-all"] = None
    for i, series in df.iterrows():
        sub = series["study_id"]
        sub_path = reconall_path / sub
        has_reconall = sub_path.exists() and any(sub_path.glob("*"))
        df.loc[i, f"Recon-all"] = has_reconall
        print(".", end="", flush=True)
    print(f"\n Saving Recon-All CSV file to {csv_fname.resolve()}")
    return df

def find_log_file(log_path):
    runs = list(log_path.glob("*"))
    if not runs:
        raise ValueError(f"No runs found in {log_path}")
    runs = [p for p in runs if p.is_dir()] # Filter out any files
    run = runs[0]
    return run

def load_nibabies_toml(log_path):
    """Load the NiBabies toml file.
    
    Parameters
    ----------
    log_path : Path
        Path to the log folder for the subject, in the Nibabies Derivative directory.
    
    Returns
    -------
    dict
        Dictionary of the toml file.
    """
    run = find_log_file(log_path)
    toml_file = run / "nibabies.toml"
    if toml_file.exists():
        return toml.load(toml_file)
    else:
        raise ValueError(f"No toml file found in {run}")


def extract_processing_datetime(log_path):
    """Extract the processing datetime from the log file."""
    folder_name = find_log_file(log_path).name
    date_match = re.search(r"\d{8}", folder_name)
    if date_match:
        date_str = date_match.group()
        date = datetime.strptime(date_str, "%Y%m%d")
        return date
    else:
        raise ValueError(f"No date found in {folder_name}")


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Build MRI Tracking CSV File.")
    parser.add_argument("session", choices=SESSION_CHOICES, help="MRI session (newborn or six_month)")
    args = parser.parse_args()

    # Call process_mri_data function with the specified session
    build_acquisition_csv(args.session)
    build_derivatives_csv(args.session)
    print("✅ Done!")
