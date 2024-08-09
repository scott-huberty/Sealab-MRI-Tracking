from pathlib import Path

# Path to the root directory of the project
ROOT_DIR = Path(__file__).resolve() # .parents[1]
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
    nibabies_path = derivatives_path / "nibabies"
    keys = ["project", "bids", "derivatives", "nibabies"]
    paths = [project_path, bids_path, derivatives_path, nibabies_path]
    assert len(keys) == len(paths)
    path_dict = dict(zip(keys, paths))
    return path_dict


