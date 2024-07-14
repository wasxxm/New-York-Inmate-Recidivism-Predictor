import importlib.util
import os

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Path to the helper_functions.py file
helper_functions_path = os.path.join(current_dir, '../helper_functions.py')

# Import helper functions
helper_spec = importlib.util.spec_from_file_location('helper_functions', helper_functions_path)
helper_module = importlib.util.module_from_spec(helper_spec)
helper_spec.loader.exec_module(helper_module)

query = """
SELECT
    c.joid::INT AS entity_id, 
    --cd.as_of_date,
    cd.as_of_date AS knowledge_date,
    cd.as_of_date - MAX(c.booking_date_full) AS time_since_last_booking
FROM 
    cleaned.jocojimsinmatedata c
JOIN 
    {most_recent_cohort_table_name} cd
ON 
    c.joid::INT = cd.entity_id
GROUP BY 
    c.joid, cd.as_of_date
HAVING 
    MAX(c.booking_date_full) <= cd.as_of_date
ORDER BY 
    as_of_date ASC
""" 
helper_module.create_time_since_x_table(query, event_name="time_since_last_booking", table_name="feature_time_since_last_booking", db_engine=helper_module.create_db_engine())
