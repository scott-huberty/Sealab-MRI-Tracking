from pathlib import Path

import paths as p


def create_participant_df(subjects_path):
    """Pass the participant IDS from project/session/bids to a dataframe.
    
    Parameters
    ----------
    subjects_path : pathlib.Path
        The path to a BIDS-like directory containing the participant folders.
    """
    # Extract the sub-* foldernames and write to file for later use
    participant_list = get_participant_list(subjects_path)
    df = pd.DataFrame(participants_list, columns=["study_id"])
    return df

def get_participant_list(directory, command=None):
    """Get a list of the participant folders in a BIDS-like a directory.
    
    Parameters
    ----------
    directory : pathlib.Path
        The directory containing the participant folders.
    """
    command = f"ls -d {directory / 'sub-*'} | grep -v '\.html$' | xargs -n1 basename"
    output = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, text=True)
    return output.stdout.strip().split("\n")

def build_acquisition_csv(project, session):
    """Build a CSV file documenting which participants received MRI scans."""
    print("👇 Documenting which participants received MRI scans 👇")
    bpath = p.get_paths(project, session)["bids"]
    df = create_participant_df(bpath)

    df[f"Anatomical"] = None
    df[f"T1w"] = None
    df[f"T2w"] = None
    df[f"Functional"] = None
    df[f"DWI"] = None
    for i, series in df.iterrows():
        sub = series["study_id"]

        assert sub.startswith("sub-")
        sub_path = bpath / sub

        assert session in ["newborn", "sixmonth", "twelvemonth"]
        anat_path = sub_path / f"ses-{ses}" / "anat"
        func_path = sub_path / f"ses-{ses}" / "func"
        dwi_path = sub_path / f"ses-{ses}" / "dwi"

        assert anat_path.exists()
        assert func_path.exists()
        assert dwi_path.exists()
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
    csv_fname = Path(f"acquisition_{session}.csv")
    save_df_to_csv(df, csv_fname)


def build_derivatives_csv(project, session):
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
    csv_fname = Path(f"./csv/derivatives_{project}_{session}.csv")    
    save_df_to_csv(df, csv_fname)
    return df

def build_nibabies_csv(project, session):
    """ Build a CSV File for Nibabies derivatives."""
    print("👇 Documenting which subjects were processed with Nibabies! 👇")
    nibabies_path = p.get_paths(project, session)["nibabies"]
    df = create_participant_df(nibabies_path)
    
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

        # for BABIES, check if subject in SI_data
        if project == "BABIES":
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
    csv_fname = Path(f"nibabies_{project}_{session}.csv")
    save_df_to_csv(df, csv_fname)
    print("✅ Done!")
    return df


def build_SI_data_df(session):
    """Build a CSV File for SI data.
    
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
    SI_PATH = SERVER_PATH / "SI_data" / "derivatives"/ "nibabies_new"
    df = create_participant_df(SI_PATH)

    df[f"SI_data"] = None
    df[f"Volume"] = None
    df[f"Cifti"] = None
    for i, series in df.iterrows():
        sub = series["study_id"]
        assert sub.startswith("sub-")
        sub_path = SI_PATH / sub
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

def build_dwi_csv(project, session):
    """Build a CSV File for DWI derivatives."""
    # Extract the sub-* foldernames and write to file for later
    # Nibabies
    print("👇 Documenting which subjects Have processed DWI data! 👇")
    dwi_path = p.get_paths(project, session)["derivatives"] / "Diffusion"
    df = create_participant_df(dwi_path)
    if df.empty:
        print(f"No participants found in {dwi_path}")
        return df

    df[f"DWI"] = None
    for i, series in df.iterrows():
        sub = series["study_id"]
        sub_path = dwi_path / sub
        assert sub_path.exists()

        assert session in ["newborn", "sixmonth", "twelvemonth"]

        has_dwi = sub_path.exists() and any(sub_path.glob("*"))
        df.loc[i, f"DWI"] = has_dwi
        print(".", end="", flush=True)
    print(f"\n Saving DWI CSV file to {csv_fname.resolve()}")
    return df


def build_precomputed_df(project, session):
    """Build a CSV File for Precomputed derivatives."""
    print("👇 Documenting which subjects Have a Manually Segmented Scan! 👇")
    precomputed_path = p.get_paths(project, session)["derivatives"] / "precomputed"
    if not precomputed_path.exists():
        print(f"No precomputed data found in {precomputed_path}")
        return pd.DataFrame()
    df = create_participant_df(precomputed_path)

    df[f"Precomputed"] = None
    for i, series in df.iterrows():
        sub = series["study_id"]
        assert sub.startswith("sub-")
        sub_path = precomputed_path / sub
        assert sub_path.exists()
        has_precomputed = sub_path.exists() and any(sub_path.glob("*"))
        df.loc[i, f"Precomputed"] = has_precomputed
        print(".", end="", flush=True)
    return df

def build_reconall_df(proejct, session):
    """Build a CSV File for Recon-All derivatives."""
    print(f"👇 Documenting which subjects Have a Recon-All Derivative! 👇")
    reconall_path = p.get_paths(project, session)["derivatives"] / "recon-all"
    if not reconall_path.exists():
        print(f"No recon-all data found in {reconall_path}")
        return pd.DataFrame()
    df = create_participant_df(reconall_path)

    df[f"Recon-all"] = None
    for i, series in df.iterrows():
        sub = series["study_id"]
        assert sub.startswith("sub-")
        sub_path = reconall_path / sub
        assert sub_path.exists()
        has_reconall = sub_path.exists() and any(sub_path.glob("*"))
        df.loc[i, f"Recon-all"] = has_reconall
        print(".", end="", flush=True)
    return df


def starting_msg(project, session, step):
    """Print a message to the console."""
    print(f"👇 Documenting which {project}-{session} subjects Have {step} data! 👇")

def save_df_to_csv(df, csv_fname):
    """Save a dataframe to a CSV file."""
    out_path = p.ROOT_DIR / "csv" / csv_fname
    print(f"Saving CSV file to {out_path.resolve()}")
    df.to_csv(out_path, index=False)

