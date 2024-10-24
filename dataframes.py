import argparse 

from pathlib import Path

import pandas as pd
from paths import SERVER_PATH, get_paths
from utils import (
    create_participant_df,
    extract_processing_datetime,
    load_nibabies_toml,
    print_starting_msg,
    save_df_to_csv,
)


def build_acquisition_df(project, session):
    """Build a CSV file documenting which participants received MRI scans."""
    print_starting_msg(project, session, "Acquired Anatomical, Functional, and DWI")
    bpath = get_paths(project, session)["bids"]
    df = create_participant_df(bpath)

    df["Anatomical"] = None
    df["T1w"] = None
    df["T2w"] = None
    df["Functional"] = None
    df["DWI"] = None
    if project == "BABIES":
        df["T2w-Focused"] = None
        df["qMRI"] = None
    for i, series in df.iterrows():
        sub = series["study_id"]

        assert sub.startswith("sub-")
        sub_path = bpath / sub

        assert session in ["newborn", "sixmonth", "twelvemonth"]
        anat_path = sub_path / f"ses-{session}" / "anat"
        func_path = sub_path / f"ses-{session}" / "func"
        dwi_path = sub_path / f"ses-{session}" / "dwi"
        # sometimes files live in here but not in anat_path
        anat_raw_path = sub_path / f"ses-{session}" / "anat_raw"
        anat_archive_path = sub_path / f"ses-{session}" / "anat_temp_archive"

        has_t1w = any(anat_path.glob("*_T1w.*"))
        has_t2w = any(anat_path.glob("*_T2w.*"))
        if not has_t1w and not has_t2w:
            if anat_raw_path.exists():
                has_t1w = any(anat_raw_path.glob("*_T1w.*"))
                has_t2w = any(anat_raw_path.glob("*_T2w.*"))

        df.loc[i, "T1w"] = has_t1w
        df.loc[i, "T2w"] = has_t2w
        df.loc[i, "Anatomical"] = has_t1w or has_t2w

        has_func = any(func_path.glob("*_bold.*"))
        df.loc[i, "Functional"] = has_func

        has_dwi = any(dwi_path.glob("*_dwi.*"))
        df.loc[i, "DWI"] = has_dwi

        if project == "BABIES":
            # T2w Focused
            check1 = any(anat_path.glob("*Blackford*"))
            check2 = any(anat_raw_path.glob("*Blackford*"))
            check3 = any(anat_archive_path.glob("*Blackford*"))
            has_t2w_focused = check1 or check2 or check3
            df.loc[i, "T2w-Focused"] = has_t2w_focused
            # qMRI search
            check1 = any(anat_path.glob("*_VFA*"))
            check2 = any(anat_raw_path.glob("*_VFA*"))
            check3 = any(anat_archive_path.glob("*_VFA*"))
            has_qmri = check1 or check2 or check3
            df.loc[i, "qMRI"] = has_qmri

        print(".", end="", flush=True)

    # Save file
    save_df_to_csv(df, project, session, "acquisition")


def build_derivatives_df(project, session):
    """ Build a CSV File for Nibabies, precomputed, and other derivatives."""
    # Extract the sub-* foldernames and write to file for later
    # Nibabies
    nibabies_df = build_nibabies_df(project, session)
    dwi_df = build_dwi_df(project, session)
    precomputed_df = build_precomputed_df(project, session)
    reconall_df = build_reconall_df(project, session)
    # Merge the dataframes
    df = nibabies_df.merge(dwi_df, on="study_id", how="outer")
    df = df.merge(precomputed_df, on="study_id", how="outer")
    df = df.merge(reconall_df, on="study_id", how="outer")
    # Save file
    save_df_to_csv(df, project, session, "derivatives")

    # Finally, build Nibabies auto report but save it separately
    save_df_to_csv(
        build_nibabies_df(project, session, auto=True),
        project,
        session,
        stage="nibabies-auto"
    )
    return df

def build_nibabies_df(project, session, auto=False):
    """ Build a CSV File for Nibabies derivatives."""
    print_starting_msg(project, session, "Processed Nibabies")

    key = "nibabies_auto" if auto else "nibabies"
    nibabies_path = get_paths(project, session)[key]
    df = create_participant_df(nibabies_path)
    if project == "BABIES" and session == "newborn":
        SI_df = build_SI_data_df(session)
    
    df["Anatomical"] = None
    df["Surface-Recon-Method"] = None
    df["Functional-Volume"] = None
    df["Functional-Surface"] = None

    for i, series in df.iterrows():
        sub = series["study_id"]
        assert sub.startswith("sub-")
        sub_path = nibabies_path / sub
        assert sub_path.exists()

        assert session in ["newborn", "sixmonth", "twelvemonth"]
        ses_path = sub_path / f"ses-{session}"
        
        anat_path = ses_path / "anat"
        func_path = ses_path / "func"
        # Have to load the toml file from the log folder
        # We use the most recent run to specify the surface recon method
        log_path = ses_path / "log"
        assert log_path.exists()
        toml_data = load_nibabies_toml(log_path)

        has_anat = anat_path.exists() and any(anat_path.glob("*"))
        df.loc[i, f"Anatomical"] = has_anat

        has_func = func_path.exists() and any(func_path.glob("*"))
        has_volume = func_path.exists() and any(func_path.glob("*_boldref.nii.gz"))
        has_cifti = func_path.exists() and any(func_path.glob("*k_bold.dtseries.nii*"))

        # for BABIES newborn, check if subject in SI_data
        if project == "BABIES" and session == "newborn":
            exists_in_SI = sub in SI_df["study_id"].values
            has_SI_volume = exists_in_SI and SI_df.loc[SI_df["study_id"] == sub, "Volume"].values[0]
            has_SI_cifti = exists_in_SI and SI_df.loc[SI_df["study_id"] == sub, "Cifti"].values[0]
            has_volume = has_volume or has_SI_volume
            has_cifti = has_cifti or has_SI_cifti
        df.loc[i, f"Functional-Volume"] = has_volume
        df.loc[i, f"Functional-Surface"] = has_cifti

        # Check for surface recon method
        recon_method = toml_data["workflow"]["surface_recon_method"]
        df.loc[i, f"Surface-Recon-Method"] = recon_method

        # Extract the processing date
        processing_date = extract_processing_datetime(log_path)
        df.loc[i, "Date-Processed"] = processing_date
        print(".", end="", flush=True)
    # Save file
    return df


def build_SI_data_df(session):
    """Build a df File for BABIES SI data.
    
    Parameters
    ----------
    session : str
        The session to process (e.g., "newborn", "sixmonth", "twelvemonth").
    
    Notes
    -----
    For the BABIES study, Sanjana processed the newborn data with Nibabies
    for Volume (i.e subcortical) outputs only. So we want to check this for
    each participant and include it in the processing counts in a separate column.
    """
    project_path =  get_paths("BABIES", session)["project"].parent
    si_path = project_path / "SI_data" / "derivatives"/ "nibabies_new"
    assert si_path.exists()
    df = create_participant_df(si_path)

    df[f"SI_data"] = None
    df[f"Volume"] = None
    df[f"Cifti"] = None
    for i, series in df.iterrows():
        sub = series["study_id"]
        assert sub.startswith("sub-")
        sub_path = si_path / sub
        assert sub_path.exists()

        assert session in ["newborn", "sixmonth", "twelvemonth"]
        func_path = sub_path / f"ses-{session}" / "func"
        has_SI_data = func_path.exists() and any(sub_path.glob("*"))
        df.loc[i, f"SI_data"] = has_SI_data
        has_volume = func_path.exists() and any(func_path.glob("*_boldref.nii.gz"))
        has_cifti = func_path.exists() and any(func_path.glob("*k_bold.dtseries.nii*"))
        df.loc[i, f"Volume"] = has_volume
        df.loc[i, f"Cifti"] = has_cifti
    return df

def df_is_empty(df):
    return df.empty or (len(df) == 1 and not df["study_id"].item())

def build_dwi_df(project, session):
    """Build a CSV File for DWI derivatives."""
    print_starting_msg(project, session, "Processed DWI")
    dpath = get_paths(project, session)["derivatives"]
    if project == "BABIES":
        dwi_path = dpath / "Diffusion"
    elif project == "ABC":
        dwi_path = dpath / "diffusion"
    df = create_participant_df(dwi_path)
    df[f"DWI"] = None

    if df.empty:
        print(f"No participants found in {dwi_path}")
        return df

    for i, series in df.iterrows():
        sub = series["study_id"]
        assert sub.startswith("sub-")
        sub_path = dwi_path / sub
        assert sub_path.exists()

        assert session in ["newborn", "sixmonth", "twelvemonth"]

        has_dwi = sub_path.exists() and any(sub_path.glob("*"))
        df.loc[i, f"DWI"] = has_dwi
        print(".", end="", flush=True)
    return df


def build_precomputed_df(project, session):
    """Build a CSV File for Precomputed derivatives."""
    print_starting_msg(project, session, "Manualy edited Anatomical Segmentation")

    precomputed_path = get_paths(project, session)["derivatives"] / "precomputed"
    if not precomputed_path.exists():
        print(f"No precomputed data found in {precomputed_path}")
        return pd.DataFrame(columns=["study_id", "Precomputed"])
    df = create_participant_df(precomputed_path)
    df[f"Precomputed"] = None

    if df.empty:
        print(f"No participants found in {precomputed_path}")
        return df

    for i, series in df.iterrows():
        sub = series["study_id"]
        assert sub.startswith("sub-")
        sub_path = precomputed_path / sub
        assert sub_path.exists()
        has_precomputed = sub_path.exists() and any(sub_path.glob("*"))
        df.loc[i, f"Precomputed"] = has_precomputed
        print(".", end="", flush=True)
    return df


def build_reconall_df(project, session):
    """Build a CSV File for Recon-All derivatives."""
    print_starting_msg(project, session, "Recon-All")

    reconall_path = get_paths(project, session)["derivatives"] / "recon-all"
    if not reconall_path.exists():
        print(f"No recon-all data found in {reconall_path}")
        return pd.DataFrame(columns=["study_id"])
    df = create_participant_df(reconall_path)
    df[f"Recon-all"] = None

    if df.empty:
        print(f"No participants found in {dwi_path}")
        return df

    for i, series in df.iterrows():
        sub = series["study_id"]
        assert sub.startswith("sub-")
        sub_path = reconall_path / sub
        assert sub_path.exists()
        has_reconall = sub_path.exists() and any(sub_path.glob("*"))
        df.loc[i, f"Recon-all"] = has_reconall
        print(".", end="", flush=True)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project",
        dest="project",
        required=True,
        type=str,
        choices=["BABIES", "ABC"]
        )
    parser.add_argument(
        "--session",
        dest="session",
        required=True,
        type=str,
        choices=["newborn", "sixmonth", "twelvemonth"]
        )

    args = parser.parse_args()
    build_acquisition_df(args.project, args.session)
    build_derivatives_df(args.project, args.session)