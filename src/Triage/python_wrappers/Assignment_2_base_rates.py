import yaml
import pandas as pd
import numpy as np
from sqlalchemy.engine.url import URL
from triage.util.db import create_engine
from triage.experiments import MultiCoreExperiment
from sqlalchemy import text
import os 
from sqlalchemy.event import listens_for
from sqlalchemy.pool import Pool
import argparse
import sys

import imp


# SET CONFIG FOR MODEL RUN:

# name of YAML file (should be in .../Triage/config_files)
config_file_name = 'assignment_2.yaml'


# ------------------------------------------------------------------

#Directory setup
curr_dir = os.getcwd()
base_dir = '/'.join( [curr_dir.split('mlpolicylab_fall23_mcrt2')[0]
                         ,'mlpolicylab_fall23_mcrt2' 
                         , curr_dir.split('mlpolicylab_fall23_mcrt2')[1] 
                         ,'mlpolicylab_fall23_mcrt2'
                         ])

#Finish file pathing
yaml_subdir = 'Triage/config_files/archive'
yaml_file_path = os.path.join(base_dir, yaml_subdir, config_file_name) #Which yaml file to use

## Set vars for manual SQL pull
sql_labels_subdir = 'Triage/sql'
sql_labels_path = os.path.join(base_dir, sql_labels_subdir, 'labels/assignment_2_labels.sql') #Which labels script to use

# ------------------------------------------------------------------

#SETUP

#Change dir to triage so that Run.py works
os.chdir( os.path.join(base_dir, 'Triage') ) 

#Load user function
python_subdir = 'Triage/python_wrappers'
run_triage = imp.load_source('run_triage', os.path.join( base_dir, python_subdir, 'Run.py') )


# ------------------------------------------------------------------


# TRIAGE


try:
    print("-"*80, '\nRunning Triage\n', '-'*80)
    run_triage.run_triage( yaml_file_path, 'mlpolicylab_fall23_mcrt2_database', 'triage_output') 
except:
    print("\n\nTriage didn't finish running\n\n")



# ------------------------------------------------------------------

# MANUAL SQL PULLS

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
db_engine = create_engine(db_url)

#Get labels script for triage
with open(sql_labels_path, 'r') as file:
    labels_sql = file.read()

#Set different time configs we want to run
configs = []
configs.append( {'as_of_date' : "2017-01-01", 
                 'label_timespan':"1yr"}
               )
configs.append( {'as_of_date' : "2017-01-01", 
                 'label_timespan':"6 months"}
               )

configs.append( {'as_of_date' : "2018-01-01", 
                 'label_timespan':"1yr"}
               )
configs.append( {'as_of_date' : "2018-01-01", 
                 'label_timespan':"6 months"}
               )


print("-"*80)
print("\nDoing Manual SQL Pulls to compare to Triage using script:\n \'{}\'\n\n".format(sql_labels_path) )
for c in configs:
    labels_sql_full = labels_sql.format( **c )
    #correctly_formatted = text(labels_sql_full)
    c['output'] = pd.read_sql(labels_sql_full, db_engine)

fstring = "{:<15} - {:<15} - {:<15} - {:<15} - {:<15} - {:<15} - {:<15}"

print(fstring.format('As of date', 'Timespan', 'Nrow' ,'Distinct IDs', 'Y=1', 'Y=0', 'Null'))
print('-'*130)
for c in configs:
    print(fstring.format(
        c['as_of_date'],
        c['label_timespan'],
        c['output'].shape[0],
        len( np.unique( c['output']['entity_id'] )),
        c['output'][ c['output']['outcome']==1].shape[0],
        c['output'][ c['output']['outcome']==0].shape[0],
        c['output']['outcome'].isnull().sum()
    ))

# ------------------------------------------------------------------

# RETRIEVE TRIAGE RESULTS

#Get results from latest triage run
latest_traige = 'SELECT cohort_table_name, labels_table_name, start_time, os_user FROM triage_metadata.triage_runs ORDER BY start_time DESC LIMIT 1'
latest_run_tables = pd.read_sql(latest_traige, db_engine) 



labels_table_name = latest_run_tables.loc[0, 'labels_table_name']
triage_user = latest_run_tables.loc[0, 'os_user']
triage_time = latest_run_tables.loc[0, 'start_time']

print("\n\nGetting labels from table: {}\n\n".format(labels_table_name) )

#Get labels table
labels_query = """
SELECT as_of_date, 
        label_timespan, 
        count(entity_id) AS nrow, 
        count(distinct entity_id) as distinct_nrow,
        sum(label) AS y_is_1, 
        100*sum( label)/count(entity_id) AS PCT_is_1
        from public.{}
GROUP BY as_of_date, label_timespan
""".format(labels_table_name)

labels_results = pd.read_sql(labels_query, db_engine) 

print("Results from latest triage run at time \'{}\' by user \'{}\':".format(triage_time, triage_user))
print("-"*80)

print(labels_results)