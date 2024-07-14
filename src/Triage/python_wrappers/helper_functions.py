import os 
import yaml 
from sqlalchemy.engine.url import URL
from sqlalchemy import text
from triage.util.db import create_engine
import typing

#For visualize time chops
from triage.component.timechop.plotting import visualize_chops
from triage.component.timechop import Timechop
import matplotlib
matplotlib.use('Agg')
import matplotlib.dates as md
import numpy as np
from triage.util.conf import convert_str_to_relativedelta
import matplotlib.pyplot as plt
import yaml
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from datetime import datetime
import yaml
import pandas as pd

def create_db_engine():

    #Set up database connection 
    andrew_id = os.getenv('USER')
    user_path = os.path.join('/mnt/data/users/', andrew_id)

    #Get dbfile path, open, edit
    dbfile = os.path.join(user_path, 'database.yaml')
    with open(dbfile, 'r') as dbf:
        dbconfig = yaml.safe_load(dbf)
    dbconfig['db'] = 'mlpolicylab_fall23_mcrt2_database'

    # creating database engine
    db_url = URL(
        'postgres',
        host=dbconfig['host'],
        username=dbconfig['user'],
        database=dbconfig['db'],
        password=dbconfig['pass'],
        port=dbconfig['port'],
    )

    return create_engine(db_url)

def get_subdirs():
    curr_dir = os.getcwd()
    base_dir =  ''.join( [os.getcwd().split('mlpolicylab_fall23_mcrt2')[0]
                         ,'mlpolicylab_fall23_mcrt2' 
                         , os.getcwd().split('mlpolicylab_fall23_mcrt2')[1] 
                         ,'mlpolicylab_fall23_mcrt2'
                         ])
    
    python_dir = os.path.join(base_dir, 'Triage/python_wrappers')
    sql_dir = os.path.join(base_dir, 'Triage/sql')
    yaml_dir = os.path.join(base_dir, 'Triage/config_files')
    
    return (python_dir, sql_dir, yaml_dir)

def read_yaml(file_name):
    with open(file_name, 'r') as file:
        return yaml.safe_load(file)
    
def build_config( meta_fp: str, 
                  temporal_fp: str,
                  label_fp: str,
                  feature_fps: typing.List[str],
                  scoring_fp: str,
                  importance_fp: str,
                  model_grid_fp: str
                  ):

    python_dir, sql_dir, yaml_dir = get_subdirs()

    # List of file names to be concatenated
    input_yamls = []
    input_yamls.append(  {'name':'metadata',  'fps' : [os.path.join(yaml_dir, 'meta_data', meta_fp) ]}  )
    input_yamls.append(  {'name':'temporal',  'fps' : [os.path.join(yaml_dir, 'temporal', temporal_fp) ]} )
    input_yamls.append(   {'name':'labels',  'fps' : [os.path.join(yaml_dir, 'labels_cohorts', label_fp) ]} )

    #input_yamls.append( {'name':'cohort', 'fps': [os.path.join(yaml_dir, 'labels_cohorts', 'cohort.yaml')]}})

    feature_fps_full = [ os.path.join(yaml_dir, 'features', fp) for fp in feature_fps ]
    input_yamls.append(  {'name':'features',  'fps' :feature_fps_full } )
    
    input_yamls.append(  {'name':'scoring',  'fps' : [os.path.join(yaml_dir, 'scoring', scoring_fp)]}  )
    input_yamls.append( {'name':'importance',  'fps' : [os.path.join(yaml_dir, 'importance', importance_fp)]}  )
    input_yamls.append(  {'name':'modelgrid',  'fps' :[os.path.join(yaml_dir, 'model_grid', model_grid_fp)] } )


    concatenated_yaml = ''
    for dd in input_yamls:
        if dd['name'] == 'features':
            concatenated_yaml += 'feature_aggregations:\n'
            concatenated_yaml += '  -\n'

        for fp in dd['fps']:
            with open(fp, 'r') as file:
                file_content = file.read()    
                concatenated_yaml += '# From: ' + '/'.join(fp.split('/')[-3:]) + '\n'
                concatenated_yaml += file_content
                concatenated_yaml += '\n\n'

    return concatenated_yaml


def get_most_recent_file(directory):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    if not files:
        return None

    most_recent_file = max(files, key=os.path.getctime)
    return os.path.join(directory, most_recent_file)


def visualize_chops_plotly( yaml_path, selected_splits=None, show_label_timespans=True, show_boxes=True, show_annotations=True):
    """Visualize time chops of a given Timechop object using plotly

    Args:
        chopper (triage.component.timechop.Timechop): A fully-configured Timechop object
        selected_splits (list): Indices of train-val sets to plot. E.g. [0, 1, 2] plots the 3 most recent splits, [0,-1] plots the first and last splits.
            Defaults to None, which plots all splits.
        show_label_timespans (bool): Whether or not to draw horizontal lines to show label timespan
            for as-of-times
        show_boxes (bool): Whether or not to show a rectangle highlighting train-test matrices
        show_annotations (bool): Whether or not to add annotations on the latest split, showing what each of the timechop parameters mean
    """

    with open(yaml_path, 'r') as fin:
            config = yaml.safe_load(fin)
    chopper = Timechop(**config['temporal_config'])
    
    # artifically set the feature_start_time to the label_start_time to zoom in on the train/validation splits
    chopper.feature_start_time = chopper.label_start_time

    chops = chopper.chop_time()
    chops.reverse() # reverse to get the most recent set first

    # Subset to relevant splits if arg specified, and generate titles for each split
    if selected_splits is not None:
      chops = [chops[i] for i in selected_splits]
      titles = tuple(f"Train-Validation Split {i+1}" for i in selected_splits)
    else:
      titles = tuple(f"Train-Validation Split {i+1}" for i in range(len(chops)))

    fig = make_subplots(rows=len(chops),
                        cols=1,
                        shared_xaxes=True,
                        shared_yaxes=True,
                        vertical_spacing=0.05,
                        subplot_titles=titles) # adds titles for each subplot

    # For each train-val split
    for idx, chop in enumerate(chops):
        train_as_of_times = chop["train_matrix"]["as_of_times"]
        test_as_of_times = chop["test_matrices"][0]["as_of_times"]

        test_label_timespan = chop["test_matrices"][0]["test_label_timespan"]
        training_label_timespan = chop["train_matrix"]["training_label_timespan"]

        # Colors for train/test
        train_color = "rgba(3, 37, 126" # dark blue (left open because we add an opacity argument below)
        test_color = "rgba(139, 0, 0" # magenta (left open because we add an opacity argument below)
        as_of_date_marker_opacity = ', 1)' # the extra ', 1)' defines opacity. 100% solid for markers
        label_line_opacity = ', 0.3)' # 30% opacity for the label lines
        rectangle_fill_opacity = ', 0.15)' # 15% opacity for rectangle fill

        train_as_of_date_color = train_color + as_of_date_marker_opacity
        train_label_period_color = train_color + label_line_opacity
        train_rectangle_fill = train_color + rectangle_fill_opacity
        test_as_of_date_color = test_color + as_of_date_marker_opacity
        test_label_period_color = test_color + label_line_opacity
        test_rectangle_fill = test_color + rectangle_fill_opacity

        # Show legend only if idx = 0 (i.e. first train-val set we are displaying)
        if idx == 0:
          # Train set as-of-date markers
          fig.add_trace(
              go.Scatter(x=[x.date() for x in train_as_of_times],
                        y=[x for x in range(len(train_as_of_times))],
                        mode='markers',
                        marker=dict(color=train_as_of_date_color),
                        name='Training as-of-date',
                        showlegend=True,
                        hovertemplate="%{x}<extra></extra>" # the extra extra tag gets rid of a default 'trace' line in the hover output and just shows 'x', the date
                        ),
              row=idx+1, # row and column of the subplots to add this trace object to
              col=1
              )
          # Validation set as-of-date markers
          fig.add_trace(
            go.Scatter(x=[x for x in test_as_of_times],
                      y=[x for x in range(len(test_as_of_times))],
                      mode='markers',
                      name='Validation as-of-date',
                      showlegend=True,
                      marker=dict(color=test_as_of_date_color),
                      hovertemplate="%{x}<extra></extra>"),
            row=idx+1,
            col=1
            )
        # Suppress legend if not the first subplot; only difference with above is showlegend=False (note, anytime we add a trace, we have to set showlegend=False to suppress useless info in the legend)
        else:
          # Train set as-of-date markers
          fig.add_trace(
              go.Scatter(x=[x.date() for x in train_as_of_times],
                        y=[x for x in range(len(train_as_of_times))],
                        mode='markers',
                        marker=dict(color=train_as_of_date_color),
                        name='Training as-of-date',
                        showlegend=False,
                        hovertemplate="%{x}<extra></extra>" # the extra extra tag gets rid of a default 'trace' line in the hover output and just shows 'x', the date
                        ),
              row=idx+1, # row and column of the subplots to add this trace object to
              col=1
              )

          # Validation set as-of-date markers
          fig.add_trace(
            go.Scatter(x=[x for x in test_as_of_times],
                      y=[x for x in range(len(test_as_of_times))],
                      mode='markers',
                      name='Validation as-of-date',
                      showlegend=False,
                      marker=dict(color=test_as_of_date_color),
                      hovertemplate="%{x}<extra></extra>"),
            row=idx+1,
            col=1
            )


        # Add test_durations annotation if option selected
        if idx == 0 and show_annotations==True:

          # Add a dashed line to show test_durations span
          x0 = test_as_of_times[0]
          x1 = test_as_of_times[-1]
          x_mid = x0 + (x1-x0)/2
          y = -1 # place the test durations labeling below the graph
          fig.add_shape(type='line', x0=x0, x1=x1, y0=y, y1=y, line={'color': 'green'}, row=idx+1, col=1)
          fig.add_annotation(x=x_mid, y=y-1, text=f"Test duration: {chop['test_matrices'][0]['test_duration']}", showarrow=False)

        # Add label timespan lines if option selected
        if show_label_timespans is True:

          # For training as_of_dates
          for i in range(len(train_as_of_times)):
            fig.add_trace(
                go.Scatter(
                    x=[train_as_of_times[i].date(), train_as_of_times[i].date() + convert_str_to_relativedelta(training_label_timespan)],
                    y=[i,i],
                    marker=dict(color=train_label_period_color, line=dict(color=train_label_period_color)),
                    hovertemplate="%{x}<extra></extra>",
                    showlegend=False
                ),
              row=idx+1,
              col=1
            )

            # Add annotation showing train label timespan on first bar in first train-val set (if option specified)
            if i == len(train_as_of_times)-1 and idx == 0 and show_annotations==True:

              # Have the x in between the label timespan
              x0 = train_as_of_times[i].date()
              x1 = train_as_of_times[i].date() + convert_str_to_relativedelta(training_label_timespan)
              x_pos = x0 + (x1 - x0)/2

              # Position at a y-value above the bar
              y_pos = i
              fig.add_annotation(x=x_pos, y=y_pos, text='Label timespan', showarrow=True, arrowhead=1, row=idx+1, col=1)

          # For test as_of_dates
          for i in range(len(test_as_of_times)):
            fig.add_trace(
                go.Scatter(
                    x=[test_as_of_times[i].date(), test_as_of_times[i].date() + convert_str_to_relativedelta(test_label_timespan)],
                    y=[i,i],
                    marker=dict(color= test_label_period_color, line=dict(color= test_label_period_color)),
                    showlegend=False,
                    hovertemplate="%{x}<extra></extra>"),
              row=idx+1,
              col=1
            )

            # Add annotation showing test label timespan on first bar in first train-val set (if option specified)
            if i == len(test_as_of_times)-1 and idx == 0 and show_annotations==True:

                # Have the x in between the label timespan
                x0 = test_as_of_times[i].date()
                x1 = test_as_of_times[i].date() + convert_str_to_relativedelta(test_label_timespan)
                x_pos = x0 + (x1 - x0)/2

                # Position at a y-value above the bar
                y_pos = i
                fig.add_annotation(x=x_pos, y=y_pos, text='Label timespan', showarrow=True, arrowhead=1, row=idx+1, col=1)

        # Add rectangles/boxes to mark train-test matrices
        if show_boxes is True:

          # Training matrix rectangle
          # Rectangle params
          x0 = min(train_as_of_times).date()
          x1 = max(train_as_of_times).date() + convert_str_to_relativedelta(training_label_timespan)
          y = max(len(test_as_of_times), len(train_as_of_times))

          fig.add_trace(
              go.Scatter(x =[x0,x0,x1,x1,x0], y=[0,y,y,0,0],
                        fill='toself', fillcolor=train_rectangle_fill,
                        showlegend=False,
                        marker=dict(color='rgba(0,255,0,0)', line=dict(color='rgba(0,255,0,0)')), # setting 0 opacity so we don't see the lines or markers
                        hoverinfo='skip'),
              row=idx+1,
              col=1,
          )

          # #Add annotated text to the middle of the training set rectangle -> this code works, but the positioning is a bit weird, so need to tweak
          # middle_index = round(len(train_as_of_times)/2)
          # x_middle = train_as_of_times[middle_index].date() + convert_str_to_relativedelta(training_label_timespan)
          # fig.add_trace(
          #     go.Scatter(x =[x_middle], y=[y-1],
          #               mode='text',
          #               text="Training Data",
          #               marker=dict(color='rgba(0,255,0,0)', line=dict(color='rgba(0,255,0,0)')), # setting 0 opacity so we don't see the lines
          #               hoverinfo='skip'),
          #     row=idx+1,
          #     col=1,
          # )

          # Test set rectangle

          # Rectangle params
          x0 = min(test_as_of_times).date()
          x1 = max(test_as_of_times).date() + convert_str_to_relativedelta(test_label_timespan)
          y = max(len(test_as_of_times), len(train_as_of_times))

          fig.add_trace(
              go.Scatter(x =[x0,x0,x1,x1,x0], y=[0,y,y,0,0],
                        fill='toself', fillcolor=test_rectangle_fill,
                        showlegend=False,
                        marker=dict(color='rgba(0,255,0,0)', line=dict(color='rgba(0,255,0,0)')), # setting 0 opacity so we don't see the lines
                        hoverinfo='skip'),
              row=idx+1,
              col=1,
          )


    fig.update_layout(height=500, width=900, showlegend=True)
    fig.show()

def get_most_recent_cohort_table_name(db_engine):
   # -- Determine which table corresponds to most recent model run -- #
    get_recent_model_runs_qry = """
    SELECT  cohort_table_name 
            ,labels_table_name
            ,start_time
            ,os_user
            ,run_hash
    FROM triage_metadata.triage_runs 
    ORDER BY start_time DESC LIMIT 1
    """ 
    latest_run_tables = pd.read_sql(get_recent_model_runs_qry, db_engine) 

    # Unpack query results
    labels_table_name, cohort_table_name, triage_user,  triage_time, run_hash = \
                latest_run_tables.loc[0, ['labels_table_name', 'cohort_table_name', 'os_user', 'start_time', 'run_hash'] ].tolist()
    return cohort_table_name

def create_time_since_x_table(query, event_name, table_name, db_engine):
    """
    Create a table with the specified name and insert data from the provided SQL query.

    Parameters:
    event_name (str): Name of the time since last event feature name to create.
    table_name (str): Name of the table to create.
    db_engine: SQLAlchemy database engine object.
    query (str): SQL query string to execute and insert the results into the table.
    """

     # Define the SQL statement to drop the table if it exists
    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"

    # Define the SQL statement to create a table
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        entity_id INT,
        knowledge_date TIMESTAMP WITHOUT TIME ZONE,
        {event_name} INTERVAL
    );
    """

    # replace the most_recent_cohort_table_name with the actual table name in the query
    recent_cohort_table_name = get_most_recent_cohort_table_name(db_engine)

    if recent_cohort_table_name is None:
        raise ValueError("No cohort table found. Please create a cohort table first.")

    query = query.replace('{most_recent_cohort_table_name}', recent_cohort_table_name)

    # Define the SQL statement to insert data into the table
    insert_data_sql = f"""
    INSERT INTO {table_name} (entity_id, knowledge_date, {event_name})
    {query};
    """

    with db_engine.connect() as connection:
          # Execute the drop table SQL statement
        connection.execute(text(drop_table_sql))

        # Execute the create table SQL statement
        connection.execute(text(create_table_sql))

        # Execute the insert data SQL statement
        connection.execute(text(insert_data_sql))
        print(f"Data inserted into feature table {table_name}")