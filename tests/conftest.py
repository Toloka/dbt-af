import pytest


def pytest_addoption(parser):
    parser.addoption('--run-airflow-tasks', action='store_true')
    parser.addoption('--allow-hosts=127.0.0.1,127.0.1.1')


@pytest.fixture(scope='session')
def run_airflow_tasks(pytestconfig):
    return pytestconfig.getoption('--run-airflow-tasks')
