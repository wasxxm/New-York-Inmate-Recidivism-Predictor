import os
import importlib.util 
import pandas as pd
import yaml
from sqlalchemy.engine.url import URL
from triage.util.db import create_engine
from datetime import datetime

# ------------------------------------------------------------------

# SET VARS:

# Chose Config Files
meta_fp = 'baseline_header.yaml'                #Seed, config version, etc
temporal_fp = 'medium_depth.yaml'               #All temporal parameters
label_fp = 'labels_V0.yaml'                     #Label query
feature_fps = ['bookings_V0.yaml', 'positive_bjmhs_V0.yaml', 'count_bookings_V0.yaml']              #All features we want to include
scoring_fp = 'assignment_4_scoring.yaml'        #Scoring metrics
importance_fp = 'assignment_4_importances.yaml' #Feature importances
model_grid_fp = 'Assignment4_models.yaml'       #Model grid

# Re-run Triage?
re_run_triage = False


# ------------------------------------------------------------------

# SETUP

# --- Import user modules ---#
modules_dir = '/'.join( [os.getcwd().split('mlpolicylab_fall23_mcrt2')[0]
                         ,'mlpolicylab_fall23_mcrt2' 
                         , os.getcwd().split('mlpolicylab_fall23_mcrt2')[1] 
                         ,'mlpolicylab_fall23_mcrt2/Triage/python_wrappers'
                         ])
os.chdir( '/'.join( [modules_dir.split('Triage')[0], 'Triage'] )  )

#Import "Run.py"
run_triage_spec = importlib.util.spec_from_file_location('run_triage',  os.path.join( modules_dir, 'Run.py') )
run_triage_module = importlib.util.module_from_spec(run_triage_spec)
run_triage_spec.loader.exec_module(run_triage_module)

#Import helper functions
helper_spec = importlib.util.spec_from_file_location('helper_functions',  os.path.join( modules_dir, 'helper_functions.py') )
helper_module = importlib.util.module_from_spec(helper_spec)
helper_spec.loader.exec_module(helper_module)

#Get Sub Directories 
python_dir, sql_dir, yaml_dir = helper_module.get_subdirs()




# ------------------------------------------------------------------

# RUN TRIAGE

if re_run_triage:


    # BUILD NEW YAML FILE

    # Concatenate all small YAML files together to make complete config file
    concat_config = helper_module.build_config( meta_fp
                    ,temporal_fp
                    ,label_fp
                    ,feature_fps
                    ,scoring_fp
                    ,importance_fp
                    ,model_grid_fp
                    )

    # Save YAML to log directory

    # Get YAML Path name
    #Name it after user name and time/date
    concat_yaml_file_name = os.getenv('USER') + datetime.now().strftime("_%I_%M%p__%d_%m_%Y")
    yaml_fp = os.path.join( 'run_yaml_files', concat_yaml_file_name + '.yaml')
    full_yaml_fp = os.path.join( yaml_dir, yaml_fp) 
    
    #Save YAML file
    with open( full_yaml_fp , 'w') as file:
        file.write(concat_config)

    try:
        run_triage_module.run_triage( full_yaml_fp, 
                                     'mlpolicylab_fall23_mcrt2_database', 
                                     'triage_output') 
    except:
        print("\n\nTriage didn't finish running\n\n")
else:
    print("\n\nNot running Triage\n\n")

    #Get most recent yaml fp for time chops 
    full_yaml_fp = helper_module.get_most_recent_file( os.path.join(yaml_dir, 'run_yaml_files') )

# ------------------------------------------------------------------

# GET TRIAGE RESULTS

# -- Set up DB Connection -- #
db_engine = helper_module.create_db_engine()

# -- Determine which table corresponds to most recent completed model run -- #

get_recent_model_runs_qry = """
SELECT  cohort_table_name 
        ,labels_table_name
        ,start_time
        ,os_user
        ,run_hash
  FROM triage_metadata.triage_runs 
  WHERE current_status = 'completed'
 ORDER BY start_time DESC LIMIT 1
""" 
latest_run_tables = pd.read_sql(get_recent_model_runs_qry, db_engine) 

# Unpack query results
labels_table_name, cohort_table_name, triage_user,  triage_time, run_hash = \
            latest_run_tables.loc[0, ['labels_table_name', 'cohort_table_name', 'os_user', 'start_time', 'run_hash'] ].tolist()


print("\n\nGetting Config Data for run hash: {}\n\n".format(run_hash) )

yaml_qry = """
SELECT config from triage_metadata.experiments
where experiment_hash = '{}'
""".format(run_hash)

yaml_config = pd.read_sql(yaml_qry, db_engine)
temporal_config = yaml_config.loc[0, 'config']['temporal_config']
print(f"Temporal config for most recent model run by user {triage_user}, hash {run_hash}:\n", '-'*80, sep="")

# Sort temporal config by key
temporal_config = {k: temporal_config[k] for k in sorted(temporal_config)}
# Print key value pairs
for key, val in temporal_config.items():
    #print key value pairs in equally spaced columns
    print(f"{key:40} {val}")
    

# -- Print out statistics from most recent model run -- #

print("\n\nGetting info from Cohort table: {}\n\n".format(cohort_table_name) )

# Query cohort table
cohort_query = """
SELECT as_of_date, 
        count( entity_id) as total_rows, 
        count( distinct entity_id) as n_unique_entity_ids,
        sum(CASE WHEN active THEN 1 ELSE 0 END) as sum_active_rows,
        sum(CASE WHEN active THEN 0 ELSE 1 END) as sum_inactive_rows
        FROM public.{}
GROUP BY as_of_date
""".format(cohort_table_name)
cohort_results = pd.read_sql(cohort_query, db_engine) 

# Print results 

print("Results COHORT Table, latest triage run at time \'{}\' by user \'{}\':".format(triage_time, triage_user))
print("-"*80)
print(cohort_results)

#------------------------------------------------------------

print("\n\nGetting LABELS from table: {}\n\n".format(labels_table_name) )

# Query labels table
labels_query = """
SELECT as_of_date, 
        label_timespan, 
        sum(label) AS y_is_1, 
        count(entity_id) - sum(label) AS y_is_0,
        sum( CASE WHEN label IS NULL then 1 else 0 end) AS num_null,
        count(label) AS nrow,
        100*sum(label)/count(entity_id) AS PCT_is_1,
        100* (count(entity_id) - sum(label))/count(entity_id) AS PCT_is_0
        from public.{}
GROUP BY as_of_date, label_timespan
ORDER BY as_of_date
""".format(labels_table_name)
labels_results = pd.read_sql(labels_query, db_engine) 

# Print results 

print("Results Labels Table, latest triage run at time \'{}\' by user \'{}\':".format(triage_time, triage_user))
print("-"*80)
print(labels_results)





# ------  GET PRECISION @100 FOR MOST RECENT TRIAGE RUN THAT COMPLETED -----#
precision_100_path = os.path.join(sql_dir, 'fetch_results', 'precision_top_100_most_recent_run.sql')
with open(precision_100_path, 'r') as file:
    precision_qry = file.read()
eval_results = pd.read_sql(precision_qry, db_engine)

eval_results['model_type_short'] = eval_results['model_type'].apply(lambda x: x.split('.')[-1] )

print("\n\nEvaluation Results - Precision @100, latest completed triage run")
print("-"*80)
print(eval_results[['evaluation_end_time', 'model_type_short', 'stochastic_value', 'standard_deviation']])




# ------  Visualize chops  -----#

# Visualize chops
max_chops_to_try = 20
plot_success = False 

while not plot_success:
    try:
        helper_module.visualize_chops_plotly( full_yaml_fp, selected_splits=list(range(max_chops_to_try)), show_label_timespans=True, show_boxes=True, show_annotations=True)
        plot_success = True
    except:
        max_chops_to_try-=1