import yaml

import pandas as pd

from sqlalchemy.engine.url import URL
from triage.util.db import create_engine
from triage.experiments import MultiCoreExperiment
import logging

import os

from sqlalchemy.event import listens_for
from sqlalchemy.pool import Pool

import argparse
import sys



def run_triage(yaml_config_p,
               db_name,
               triage_out_p):
    
    andrew_id = os.getenv('USER')
    user_path = os.path.join('/mnt/data/users/', andrew_id)

    # add logging to a file (it will also go to stdout via triage logging config)
    log_filename = os.path.join(user_path, 'triage.log')
    logger = logging.getLogger('')
    hdlr = logging.FileHandler(log_filename)
    hdlr.setLevel(15)  # verbose level
    hdlr.setFormatter(logging.Formatter(
        '%(name)-30s  %(asctime)s %(levelname)10s %(process)6d  %(filename)-24s  %(lineno)4d: %(message)s',
        '%d/%m/%Y %I:%M:%S %p'))
    logger.addHandler(hdlr)

    # creating database engine
    dbfile = os.path.join(user_path, 'database.yaml')

    with open(dbfile, 'r') as dbf:
        dbconfig = yaml.safe_load(dbf)

    # assume group role to ensure shared permissions
    if 'role' in dbconfig:
        @listens_for(Pool, "connect")
        def assume_role(dbapi_con, connection_record):
            logging.debug(f"setting role {dbconfig['role']};")
            dbapi_con.cursor().execute(f"set role {dbconfig['role']};")

                

    db_url = URL(
        'postgres',
        host=dbconfig['host'],
        username=dbconfig['user'],
        database=dbconfig['db'],
        password=dbconfig['pass'],
        port=dbconfig['port'],
    )

    print("\nConnecting to DB\n...\n..\n.")
    try:
        db_engine = create_engine(db_url)
        print("success\n")
    except:
        print("ERROR: DB connection failed. Exiting\n");
        sys.exit(1)

    print("\nTesting DB connection:\n...\n..\n.")
    # Test DB_Engine
    try:
        trash = pd.read_sql('SELECT * FROM raw.jocojococlient LIMIT 1', db_engine)
        print("Success")
    except:
        print("ERROR: DB test failed. Exiting\n");
        sys.exit(1)

    triage_output_path = os.path.join(user_path, 'triage_output')
    os.makedirs(triage_output_path, exist_ok=True)

    # loading config file
    print("\nLoading Config file\n...\n..\n.")
    try:
        with open(yaml_config_p, 'r') as fin:
            config = yaml.safe_load(fin)
        print("Success\n")
    except:
        print("ERROR: Could not load config file \'{}\'. Exiting\n".format(yaml_config_p));
        sys.exit(1)

        # creating experiment object
    print("\nCreating Experiment Object\n...\n..\n.")
    try:
        experiment = MultiCoreExperiment(
            config=config,
            db_engine=db_engine,
            project_path=triage_output_path,
            n_processes=2,
            n_bigtrain_processes=1,
            n_db_processes=2,
            replace=True,
            save_predictions=True
        )
        print("Success\n")
    except Exception as e:
        print("ERROR: Could not Create Experiment.\nError was: \'{}\'\nExiting\n".format(e));
        sys.exit(1)

    print("Validating Experiment")
    experiment.validate()

    print("Running Experiment")
    experiment.run()


def parse_command_line_args():
    parser = argparse.ArgumentParser(description="Parse command line arguments")

    # Define the three command line arguments
    parser.add_argument("yaml_config_p", help="File path to YAML config file")
    parser.add_argument("--db_name", default="mlpolicylab_fall23_mcrt2_database",
                        help="Database Name (default: mlpolicylab_fall23_mcrt2_database)")
    parser.add_argument("--triage_out_p", default="triage_output",
                        help="File Path to Triage Output (default: triage_output)")
    parser.add_argument("--logging_p", default="triage.log", help="File Path to Logging File (default: triage.log)")
    parser.add_argument("--db_yaml", default="database.yaml",
                        help="File Path to Database YAML file (default: database.yaml)")

    # Parse the command line arguments
    args = parser.parse_args()

    return args


if __name__ == "__main__":

    if len(sys.argv) == 1:
        print("No command line arguments provided.\nPass in at least 1 argument for YAML path.\nExiting...")
        sys.exit(1)

    parsed_args = vars(parse_command_line_args())

    print("\n\n")
    print("-" * 80)
    print("Attempting to run Triage pipeline with following files:\n")
    for k, v in parsed_args.items():
        print('{:<15}:{:>50}'.format(k, v))
    print("-" * 80)

    run_triage(parsed_args['yaml_config_p'],
               parsed_args['db_name'],
               parsed_args['triage_out_p'])