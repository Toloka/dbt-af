# \[Preview\] Extras and scripts

The features listed below are currently in preview, and they are subject to removal or modification in the future as we
prioritize incorporating them into the CLI.

## Extras

- `mcd`: makes available to integrate Airflow and dbt with MontecarloData (see [tutorial](integration_with_other_tools.md)).
- `tests`: installs a script to test dbt project ([tests tutorial](#_dbt-af_-tests))
- `minidbt`: installs a script to restructure the dbt project, reducing the parsing overhead on each
  execution ([minidbt tutorial](#minidbt))
- `examples`: special extra to run tutorials.

## _dbt-af_ tests

`dbt-af` brings a lot of features into dbt project, and it's important to test them for correctness.

For now, available tests are:

1. Test that all models are presented in Airflow DAGs
2. Test that raw model config is correctly parsed by `dbt-af`
3. Test that all models and tests have unique names
4. Test that all tests have parent nodes
5. Test for typos in nodes' names
6. Test for `kubernetes` profiles

To run these tests, you need to install extra `tests` with _dbt-af_:

```bash
pip install dbt-af[tests]
```

This will make available a script `dbt-af-manifest-tests` to run the tests. Run to get mode information:

```bash
dbt-af-manifest-tests --help
```

## minidbt
Run `pip install dbt-af[minidbt]`

`minidbt` introduces a new approach to organizing your dbt project. It can be used during your CI/CD pipeline. 

It becomes more efficient when you have a large number of nodes and each dbt run takes a long time to parse the project. 
`minidbt` reorganizes the project, reducing the time needed for parsing on each execution. 

As always, there is a trade-off: `minidbt` makes each individual run faster, 
but it requires more time for the project to be reorganized during CI/CD, 
and it uses more disk space as it copies all the dependencies for each node to separate directories.

To use it, run 
```bash
mini_dbt_project_generator --help
```
