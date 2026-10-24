-- ChronoSaga Down Migration (001_initial_schema.down.sql)

DROP TABLE IF EXISTS demo_payments;
DROP TABLE IF EXISTS demo_inventory;
DROP TABLE IF EXISTS demo_effects;
DROP TABLE IF EXISTS compensation_tasks;
DROP TABLE IF EXISTS timer_tasks;
DROP TABLE IF EXISTS activity_tasks;
DROP TABLE IF EXISTS workflow_events;
DROP TABLE IF EXISTS workflow_runs;
