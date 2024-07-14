WITH most_recent_triage_run AS (
        SELECT *
          FROM triage_metadata.triage_runs 
        WHERE current_status = 'completed'
         ORDER BY start_time DESC LIMIT 1
)

, most_recent_experiment AS (
SELECT * from triage_metadata.experiments e
INNER JOIN most_recent_triage_run tr
ON tr.run_hash = e.experiment_hash

INNER JOIN triage_metadata.experiment_models em
ON e.experiment_hash = em.experiment_hash

INNER JOIN triage_metadata.models m
ON m.model_hash = em.model_hash

INNER JOIN test_results.evaluations eval
ON eval.model_id = m.model_id

where metric = 'precision@' AND parameter = '100_abs'
)
SELECT  metric, 
        parameter, 
        evaluation_end_time,
        model_type,
        hyperparameters,
        best_value,
        worst_value,
        stochastic_value,
        standard_deviation,
        time_splits,
        as_of_times, 
        total_features, 
        models_needed, 
        os_user, 
        batch_run_time
        from most_recent_experiment
order by model_type, evaluation_end_time

