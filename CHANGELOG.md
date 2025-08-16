# Changelog

## v0.14.3

Fixes:

- Include snapshots to pre-selected dbt nodes
- Rename deprecated airflow DAG argument `schedule_interval` to `schedule`

## v0.14.2

Features:
- Add support for Python 3.12 (updated requirement to `>=3.10,<3.13`)

## v0.14.1

Fixes:

- Fix invoking macro name in dbt run command

## v0.14.0

Features:

- The is_dev flag is deprecated in favor of the dry_run flag in the main
  configuration (https://github.com/Toloka/dbt-af/pull/67)
- Improve the performance of the venv operator (https://github.com/Toloka/dbt-af/pull/65)
- Enhance dbt_run_model with additional CLI options and parameters (https://github.com/Toloka/dbt-af/pull/64)
- Add iceberg compatibility (thanks @authentic7) (https://github.com/Toloka/dbt-af/pull/62)

Fixes:

- Isolates dbt source freshness run in tmp directory (https://github.com/Toloka/dbt-af/pull/67)
- Freeze time for monthly scheduling (https://github.com/Toloka/dbt-af/pull/66)
- Add more flexibility to the running of functional tests (https://github.com/Toloka/dbt-af/pull/65)

Chore:

- add a note section about medium tests configuration (https://github.com/Toloka/dbt-af/pull/67)

## v0.13.0

Features:

- Add schedule shift functionality to dbt model's config:
  ```yaml
  config:
    schedule: @hourly
    schedule_shift: 30  # any positive integer
    schedule_shift_unit: minute  # any from minute/hour/day/week
  ```

## v0.12.2

Chore:

- update `typer` requirement to `>=0.9` version

## v0.12.1

Fixes:

- fix `poetry` version in release action
- rebuild package to include all scripts

## v0.12.0

> [!WARNING]
> This release is yanked due to issues with a build process (scripts are not included in the package).

Features:

- add custom Python models to be run in a virtual environment.
- add `--debug` flag to `dbt source freshness` command, if it's enabled in the main config

## v0.11.0

Features:

- add field to the main config to specify custom dbt executable path in the main config for all dbt
  tasks (https://github.com/Toloka/dbt-af/issues/46)

Fixes:

- make less strict dependencies constrains (https://github.com/Toloka/dbt-af/issues/45)
- allow `threads` field to be string in dbt profiles parsing (https://github.com/Toloka/dbt-af/issues/44)

## v0.10.0

Features:

- add `env` field to dbt model's config to specify additional environment variables

## v0.9.3

Fixes:

- change dbt_run_model DAG name for multi-project usage (https://github.com/Toloka/dbt-af/pull/39)

## v0.9.2

Fixes:

- all dbt target types are now supported

## v0.9.1

Fixes:

- fixed bug with schedule overlap

## v0.9.0

Features:

- add new scheduling tag `@every15minutes`
- new [docs page](docs/docs.md) with code references

## v0.8.1

Fixes:

- upgrade `airflow-mcd` to 0.3.3 version to fix bug with DAG callbacks
- fix correct usage of the default retry policy if none has been passed.

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