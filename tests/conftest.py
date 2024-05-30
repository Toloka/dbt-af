import pytest


def pytest_addoption(parser):
    parser.addoption('--run-airflow-tasks', action='store_true')


@pytest.fixture(scope='session')
def run_airflow_tasks(pytestconfig):
    return pytestconfig.getoption('--run-airflow-tasks')
