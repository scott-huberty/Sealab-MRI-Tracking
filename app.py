from pathlib import Path

import dash_bootstrap_components as dbc
import duckdb
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, callback, dash_table, dcc, html
from duckdb.duckdb import DuckDBPyRelation

#################### STYLES #####################
TABLE_KWARGS = {
    "editable": False,
    "page_size": 10,
    "page_action": "none",
    "fixed_rows": {"headers": True},
    "style_header": {
        "backgroundColor": "var(--bs-dark)",  # Bootstrap primary color
        "color": "var(--bs-light)",
        "fontWeight": "bold",
        "textAlign": "center",
    },
    "style_table": {
        "height": "300px",
        "overflowX": "auto",
        "overflowY": "auto",
    },
    "style_cell": {
        "backgroundColor": "var(--bs-primary)",
        "textAlign": "left",
    },
}

############### FUNCTIONS #####################

def combine_acq_proc_dfs(df_acq: pd.DataFrame, df_proc: pd.DataFrame,) -> pd.DataFrame:
    want_cols = ["Anatomical", "Functional", "DWI"]
    df_acq = df_acq.set_index("study_id")[want_cols].copy()
    df_proc = df_proc.set_index("study_id")[want_cols].copy()

    acq_cols = pd.MultiIndex.from_tuples([("Acquired", scan) for scan in want_cols])
    proc_cols = pd.MultiIndex.from_tuples([("Processed", scan) for scan in want_cols])
    df_acq.columns = acq_cols
    df_proc.columns = proc_cols
    df_combined = pd.concat([df_acq, df_proc], axis=1)
    return df_combined

def make_dash_table(db: DuckDBPyRelation) -> dash_table.DataTable:
    df: pd.DataFrame = db.to_df()
    table = dash_table.DataTable(
        data=df.to_dict("records"),
        editable=False,
        page_size=10,
        page_action='none',
        fixed_rows={'headers': True},
        style_header={
            "backgroundColor": "var(--bs-dark)",  # Bootstrap primary color
            "color": "var(--bs-light)",
            "fontWeight": "bold",
            "textAlign": "center",
        },
        style_table={
        'height': '300px',
        'overflowX': "auto",
        'overflowY': 'auto',
        },
        style_cell={
            "backgroundColor": "var(--bs-primary)",
            "textAlign": "left",
        },
    )
    return table


def get_count_query(db_name: str) -> str:
    return f"SELECT COUNT(Anatomical) FILTER (WHERE Anatomical) AS Anatomical, COUNT(Functional) FILTER (WHERE Functional) AS BOLD, COUNT(DWI) FILTER (WHERE DWI) AS DWI FROM {db_name}"

def make_query_df(
    db_name: str,
    query: str,
    ) -> pd.DataFrame:
    # query = duckdb.sql(get_count_query(db_name))
    df = duckdb.sql(query).to_df().T.reset_index()
    if "acq" in db_name:
        return df.rename(columns={"index": "Scan", 0: "Acquired"}).set_index("Scan")
    elif "proc" in db_name:
        return df.rename(columns={"index": "Scan", 0: "Processed"}).set_index("Scan")
    else:
        raise ValueError(f"acq or proc should exist in the database name. Got: {db_name}")

def make_query_table(df: pd.DataFrame) -> dash_table.DataTable:
    return dash_table.DataTable(
        data=df.to_dict("records"),
        **TABLE_KWARGS,
    )

############# I/O #################
csv_dir = Path(__file__).parent.resolve() / "csv"
assert csv_dir.exists()
newborn_acq_fname = csv_dir / "BABIES_newborn_acquisition.csv"
sixmonth_acq_fname = csv_dir / "BABIES_sixmonth_acquisition.csv"

newborn_proc_fname = csv_dir / "BABIES_newborn_derivatives.csv"
sixmonth_proc_fname = csv_dir / "BABIES_sixmonth_derivatives.csv"

db_newborn_acq: DuckDBPyRelation = duckdb.read_csv(newborn_acq_fname)
db_sixmonth_acq: DuckDBPyRelation = duckdb.read_csv(sixmonth_acq_fname)

df_newborn_proc = pd.read_csv(newborn_proc_fname).rename(columns={"Functional-Volume": "Functional"})
df_sixmonth_proc = pd.read_csv(sixmonth_proc_fname).rename(columns={"Functional-Volume": "Functional"})

db_newborn_proc: DuckDBPyRelation = duckdb.sql("SELECT study_id, Anatomical, Functional, DWI FROM df_newborn_proc")
db_sixmonth_proc: DuckDBPyRelation = duckdb.sql("SELECT study_id, Anatomical, Functional, DWI FROM df_sixmonth_proc")
# db_newborn_proc: DuckDBPyRelation = duckdb.read_csv(newborn_proc_fname)
# db_sixmonth_proc: DuckDBPyRelation = duckdb.read_csv(sixmonth_proc_fname)

# Counts
query = get_count_query("db_newborn_acq")
query_df_newborn_acq = make_query_df("db_newborn_acq", query)

query = get_count_query("db_sixmonth_acq")
query_df_sixmonth_acq = make_query_df("db_sixmonth_acq", query=query)

query = get_count_query("db_newborn_proc")
query_df_newborn_proc = make_query_df("db_newborn_proc", query)

query = get_count_query("db_sixmonth_proc")
query_df_sixmonth_proc = make_query_df("db_sixmonth_proc", query)

newborn_df = pd.concat([query_df_newborn_acq, query_df_newborn_proc], axis=1)
sixmonth_df = pd.concat([query_df_sixmonth_acq, query_df_sixmonth_proc], axis=1)
counts_df = pd.concat([newborn_df, sixmonth_df], axis=1, keys=["Newborn", "Six Month"])

############################ COMPONENTS ############################
save_button = dbc.Button(
                    "Download CSV", id="btn_csv", color="success", className="m-1"
                    )
downloader = dcc.Download(id="download-dataframe-csv")
dropdown = dcc.Dropdown(
    ["BABIES"],
    value="BABIES",
    id="project-dropdown",
    style={"margin-top": "5px"},
    )

# DataFrames of acquired and processed scans
table_newborn_acq: dash_table.DataTable = make_dash_table(db_newborn_acq)
table_sixmonth_acq: dash_table.DataTable = make_dash_table(db_sixmonth_acq)

table_tabs = dbc.Tabs(
    id="table-tabs",
    active_tab="newborn_acq",
    children=[
        dbc.Tab(tab_id="newborn_acq", label="Newborn", children=table_newborn_acq),
        dbc.Tab(tab_id="sixmonth_acq", label="Six Month", children=table_sixmonth_acq),
    ],
)

# Scan Count Tables
newborn_table = make_query_table(newborn_df.reset_index())
sixmonth_table = make_query_table(sixmonth_df.reset_index())
query_tabs = dbc.Tabs(
    id="query-tabs",
    active_tab="newborn",
    children=[
        dbc.Tab(tab_id="newborn", label="Newborn", children=newborn_table),
        dbc.Tab(tab_id="sixmonth", label="Six Month", children=sixmonth_table),
    ]
)

# Bar Chart
y_max = counts_df.T.groupby(level=0).sum().max().max() * 1.5

# Create a figure with the right layout
fig = go.Figure(
    layout=go.Layout(
        #height=600,
        #width=1000,
        barmode="overlay",
        title="Number of Acquired and Processed Scans",
        yaxis_title="Count",
        yaxis_showticklabels=True,
        yaxis_showgrid=True,
        yaxis_range=[0, y_max],
       # Secondary y-axis overlayed on the primary one and not visible
        yaxis2=go.layout.YAxis(
            visible=False,
            matches="y",
            overlaying="y",
            anchor="x",
        ),
        font=dict(size=18),
        legend_x=0,
        legend_y=.8,
        legend_orientation="h",
        hovermode="x",
        margin=dict(b=0,t=40,l=0,r=10)
    )
)

for ii, (level, col) in zip([0,0,1,1], (counts_df.columns)):
    fig.add_bar(
        x=counts_df.index,
        y=counts_df[level][col],
        yaxis=f"y{ii + 1}",
        offsetgroup=str(ii),
        offset=(ii - 1) * 1/3,
        width=1/3,
        legendgroup=level,
        legendgrouptitle_text=level,
        name=col,
        hovertemplate="%{y}<extra></extra>",
        )

############################ APP ####################################
app = Dash(external_stylesheets=[dbc.themes.SLATE])
server = app.server

################################## LAYOUT ##################################
app.layout = dbc.Container([
    dbc.Label('SEA Lab MRI Tracking Dashboard'),
    dbc.Row(
        children=[dbc.Col(
            children=[save_button, downloader],
            md=3,
            ),
        dbc.Col(dropdown, md=8),
        ],
    ),
    dbc.Row(
        [
            dbc.Col(table_tabs, id="table-div", md=7),
            dbc.Col(
                query_tabs,
                id="query-div",
                md=5,
                style={"margin-left": "5"},
                ),
        ],
    ),
    dbc.Row(
        [
            dbc.Col(dcc.Graph(figure=fig), id="bar-div", md=7),
        ],
        style={"margin-top": "10px"},
    ),
])

################ CALLBACKS #####################
@callback(
    Output("download-dataframe-csv", "data"),
    Input("btn_csv", "n_clicks"),
    prevent_initial_call=True,
)
def func(n_clicks):
    print("Downloading CSV")
    return dcc.send_data_frame(counts_df.to_csv, "mydf.csv")

if __name__ == "__main__":
    app.run(debug=False)