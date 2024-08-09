import argparse

import dataframes


def parse_args():
    parser = argparse.ArgumentParser(description="Process MRI data.")
    parser.add_argument("project",
                        type=str,
                        required=True,
                        choices=["ABC", "BABIES",],
                        help="Project name. Must be 'ABC' or 'BABIES'.",
                        )
    parser.add_argument("session",
                        type=str,
                        required=True,
                        choices=["newborn", "sixmonth", "twelvemonth"],
                        help="Visit. Must be 'newborn', 'sixmonth', or 'twelvemonth'.",
                        )
    args = parser.parse_args()
    return args

def build_dataframes(project, session):
    dataframes.build_acquisition_df(project, session)
    dataframes.build_derivatives_df(project, session)

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    project = args.project
    session = args.session
    build_dataframes(project, session)
    print("✅ Done!")
