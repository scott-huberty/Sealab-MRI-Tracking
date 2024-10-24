from pathlib import Path

# Path to the root directory of the project
ROOT_DIR = Path(__file__).parent.resolve() # .parents[1]
SERVER_PATH = Path("/Volumes") / "HumphreysLab" / "Daily_2"

def _get_session_dir(project, session):
    assert session in ["newborn", "sixmonth", "twelvemonth"]
    if project == "BABIES" and session == "sixmonth":
        return "six_month"
    return session

def get_paths(project, session):
    ses_dir = _get_session_dir(project, session)
    project_path = SERVER_PATH / project / "MRI" / ses_dir
    bids_path = project_path / "BIDS"
    derivatives_path = project_path / "derivatives"
    nibabies_path = derivatives_path / "Nibabies"
    nibabies_auto_path = derivatives_path / "Nibabies_auto"
    keys = ["project", "bids", "derivatives", "nibabies", "nibabies_auto"]
    paths = [project_path, bids_path, derivatives_path, nibabies_path, nibabies_auto_path]
    assert len(keys) == len(paths)
    path_dict = dict(zip(keys, paths))
    return path_dict

def get_csv_paths(project):
    csv_path = ROOT_DIR / "csv"
    return {"redcap": csv_path / f"redcap_{project}.csv",
            "datadict": csv_path / f"{project}_DataDictionary.csv",
            "acquisition_newborn": csv_path / f"{project}_newborn_acquisition.csv",
            "acquisition_sixmonth": csv_path / f"{project}_sixmonth_acquisition.csv",
            "acquisition_twelvemonth": csv_path / f"{project}_twelvemonth_acquisition.csv",
            "derivatives_newborn": csv_path / f"{project}_newborn_derivatives.csv",
            "derivatives_sixmonth": csv_path / f"{project}_sixmonth_derivatives.csv",
            "derivatives_twelvemonth": csv_path / f"{project}_twelvemonth_derivatives.csv",
            }


