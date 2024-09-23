# Changelog

## v0.8.0
Features:
- remove `airflow.Dataset` from backfill DAGs
- add retry policies for all DAG components

Fixes:
- skip tableau refresh tasks if error `409093` occurs (refresh operation is already queued)
- use a correct path to read `profiles.yml` file (from issue [#19](https://github.com/Toloka/dbt-af/issues/19))

## v0.7.3
- use default connection for mcd export

## v0.7.2
- add `skipped_states` to DbtExternalSensor

## v0.7.1
- mcd: use gateway connection ID for dbt artifact upload

## v0.7.0
- update `airflow-mcd` version

## v0.6.0
- add custom callbacks for tasks and DAGs

## v0.5.3
- fix functional tests: correctly recreate dbt test operator's task_id

## v0.5.2
- fix tableau operator rendering

## v0.5.1
- downgrade `tableauserverclient` to `^0.25` version 

## v0.5.0
- update issue templates in GitHub
- add integration with tableau to refresh tasks

## v0.4.3
- add CONTRIBUTING.md
- remove maximum dependency constraint for `apache-airflow` package

## v0.4.2
- upgrade pydantic version to v2+ (with fallback to old api)

## v0.4.1
- fix: use target environment for `dbt source freshness` command

## v0.4.0
- Update requirements

## v0.1.1
- add AUTHORS file

## v0.1.0
- Initial release
- add docs and examples