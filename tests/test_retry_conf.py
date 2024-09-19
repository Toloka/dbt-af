from datetime import timedelta

import attrs
import pytest

from dbt_af.conf import RetriesConfig, RetryPolicy


@pytest.fixture
def non_standard_retry_policy():
    return RetryPolicy(retries=5, retry_delay=timedelta(seconds=10))


def test_default_retries_config():
    default_retries_config = RetriesConfig()
    for _attr in attrs.fields(RetriesConfig):
        assert getattr(default_retries_config, _attr.name) == default_retries_config.default_retry_policy


def test_retries_config_with_policy_overwrite(non_standard_retry_policy):
    config = RetriesConfig(
        default_retry_policy=RetryPolicy(retries=3, retry_delay=timedelta(seconds=5), retry_exponential_backoff=True),
        sensor_retry_policy=RetryPolicy(retries=5, retry_delay=timedelta(seconds=10)),
    )

    assert config.sensor_retry_policy == RetryPolicy(
        retries=5, retry_delay=timedelta(seconds=10), retry_exponential_backoff=True
    )
    assert config.dbt_run_retry_policy == RetryPolicy(
        retries=3, retry_delay=timedelta(seconds=5), retry_exponential_backoff=True
    )
    assert config.supplemental_task_retry_policy == RetryPolicy(
        retries=3, retry_delay=timedelta(seconds=5), retry_exponential_backoff=True
    )
