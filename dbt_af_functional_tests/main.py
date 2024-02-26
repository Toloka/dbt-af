from pathlib import Path

import pytest
import typer

cli = typer.Typer()


@cli.command()
def run_tests(
    manifest_path: str = typer.Option(exists=True),
    profiles_path: str = typer.Option(exists=True),
    dbt_project_path: str = typer.Option(exists=True),
    target: str = typer.Option(exists=True),
    etl_name: str = typer.Option(None),
):
    args = [
        Path(__file__).parent.as_posix(),
        '-q',
        '-s',
        '-vv',
        '--manifest_path',
        manifest_path,
        '--profiles_path',
        profiles_path,
        '--dbt_project_path',
        dbt_project_path,
        '--target',
        target,
    ]
    if etl_name:
        args.extend(['--etl_name', etl_name])
    raise SystemExit(pytest.main(args))


if __name__ == '__main__':
    cli()
