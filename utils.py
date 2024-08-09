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


def print_starting_msg(project, session, step):
    """Print a message to the console."""
    print(f"👇 Documenting which {project}-{session} subjects Have {step} data! 👇")

def save_df_to_csv(df, project, session):
    """Save a dataframe to a CSV file."""
    csv_fname = Path(f"acquisition_{project}_{session}.csv")
    out_path = p.ROOT_DIR / "csv" / csv_fname
    print(f"Saving CSV file to {out_path.resolve()}")
    df.to_csv(out_path, index=False)

