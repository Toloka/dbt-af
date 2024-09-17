import datetime
import logging
import uuid
from typing import Sequence

import pendulum
from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType


def node_ids(tasks):
    return sorted(t.node_id for t in tasks)


def nodes_operator_names(tasks) -> dict[str, str]:
    return {t.node_id: t.operator_name for t in tasks}


def try_run_task_in_dag(dag: DAG, task_id: str, additional_expected_ti_states: Sequence[TaskInstanceState] = []):
    start_date = pendulum.now().replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=1)
    end_date = start_date + datetime.timedelta(hours=1)
    run_id = f'test_run_{uuid.uuid4()}'
    dagrun = dag.create_dagrun(
        run_id=run_id,
        state=DagRunState.RUNNING,
        execution_date=pendulum.now(),
        data_interval=(start_date, end_date),
        start_date=end_date,
        run_type=DagRunType.MANUAL,
    )
    ti: TaskInstance = dagrun.get_task_instance(task_id=task_id)
    ti.task = dag.get_task(task_id=task_id)
    ti.run(ignore_ti_state=True, ignore_all_deps=True, verbose=False)

    # all tasks should be in success state and all sensors should be up_for_reschedule
    always_expected_states = [TaskInstanceState.SUCCESS, TaskInstanceState.UP_FOR_RESCHEDULE]
    ti_states_to_expect = always_expected_states + list(additional_expected_ti_states)
    assert ti.state in ti_states_to_expect


def run_all_tasks_in_dag(dags: dict[str, DAG], additional_expected_ti_states: Sequence[TaskInstanceState] = []):
    airflow_loggers = [logger for logger in logging.Logger.manager.loggerDict if logger.startswith('airflow')]
    for logger in airflow_loggers:
        logging.getLogger(logger).setLevel(logging.ERROR)

    for dag in dags:
        for task_id in dags[dag].task_ids:
            try_run_task_in_dag(dags[dag], task_id, additional_expected_ti_states)


def test_domain_depends_on_another_partially_has_correct_dags(
    dags_domain_depends_on_another_partially,
    run_airflow_tasks,
):
    dags = dags_domain_depends_on_another_partially

    assert sorted(dags) == ['a__backfill', 'a__daily', 'b__backfill', 'b__daily']

    a = dags['a__daily']
    b = dags['b__daily']

    assert a.dag_id == 'a__daily'
    assert sorted(a.task_ids) == ['a1', 'a2']
    assert node_ids(a.task_dict['a1'].upstream_list) == []
    assert node_ids(a.task_dict['a2'].upstream_list) == ['a1']

    assert b.dag_id == 'b__daily'
    assert sorted(b.task_ids) == ['a__daily__dependencies__group.wait__a2', 'b1', 'b2']
    assert nodes_operator_names(b.tasks) == {
        'a__daily__dependencies__group.wait__a2': 'DbtExternalSensor',
        'b1': 'DbtRun',
        'b2': 'DbtRun',
    }
    assert node_ids(b.task_dict['a__daily__dependencies__group.wait__a2'].upstream_list) == []
    assert node_ids(b.task_dict['b1'].upstream_list) == []
    assert node_ids(b.task_dict['b2'].upstream_list) == ['a__daily__dependencies__group.wait__a2', 'b1']

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_domain_depends_on_two_domains_has_correct_dags(dags_domain_depends_on_two_domains, run_airflow_tasks):
    dags = dags_domain_depends_on_two_domains

    assert sorted(dags) == ['a__backfill', 'a__daily', 'b__backfill', 'b__daily', 'c__backfill', 'c__daily']

    a = dags['a__daily']
    b = dags['b__daily']
    c = dags['c__daily']

    assert a.dag_id == 'a__daily'
    assert sorted(a.task_ids) == ['a1']
    assert node_ids(a.task_dict['a1'].upstream_list) == []

    assert b.dag_id == 'b__daily'
    assert sorted(b.task_ids) == ['b1']
    assert node_ids(b.task_dict['b1'].upstream_list) == []

    assert c.dag_id == 'c__daily'
    assert sorted(c.task_ids) == [
        'a__daily__dependencies__group.wait__a1',
        'b__daily__dependencies__group.wait__b1',
        'c1',
    ]
    assert node_ids(c.task_dict['a__daily__dependencies__group.wait__a1'].upstream_list) == []
    assert node_ids(c.task_dict['b__daily__dependencies__group.wait__b1'].upstream_list) == []
    assert node_ids(c.task_dict['c1'].upstream_list) == [
        'a__daily__dependencies__group.wait__a1',
        'b__daily__dependencies__group.wait__b1',
    ]

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_hourly_task_with_tests_has_correct_dags(dags_hourly_task_with_tests, run_airflow_tasks):
    dags = dags_hourly_task_with_tests

    assert sorted(dags) == ['a__backfill', 'a__hourly', 'a__large_tests__daily']

    a = dags['a__hourly']  # has small test
    large_test_dag = dags['a__large_tests__daily']

    assert a.dag_id == 'a__hourly'
    assert sorted(a.task_ids) == [
        'a1__group.a1',
        'a1__group.a1__end',
        'a1__group.not_null_a1_id',
        'medium_tests__a__hourly.a1__unique_a1_id',
    ]
    assert nodes_operator_names(a.tasks) == {
        'a1__group.a1': 'DbtRun',
        'a1__group.a1__end': 'EmptyOperator',
        'a1__group.not_null_a1_id': 'DbtTest',
        'medium_tests__a__hourly.a1__unique_a1_id': 'DbtTest',
    }
    assert node_ids(a.task_dict['a1__group.a1'].upstream_list) == []
    assert node_ids(a.task_dict['a1__group.not_null_a1_id'].upstream_list) == ['a1__group.a1']
    assert node_ids(a.task_dict['a1__group.a1__end'].upstream_list) == ['a1__group.not_null_a1_id']
    assert node_ids(a.task_dict['medium_tests__a__hourly.a1__unique_a1_id'].upstream_list) == ['a1__group.a1__end']

    assert large_test_dag.dag_id == 'a__large_tests__daily'
    assert sorted(large_test_dag.task_ids) == [
        'a__hourly__dependencies__group.wait__a1',
        'accepted_values_a1_id__1__2__3',
    ]
    assert nodes_operator_names(large_test_dag.tasks) == {
        'a__hourly__dependencies__group.wait__a1': 'DbtExternalSensor',
        'accepted_values_a1_id__1__2__3': 'DbtTest',
    }
    assert node_ids(large_test_dag.task_dict['a__hourly__dependencies__group.wait__a1'].upstream_list) == []
    assert node_ids(large_test_dag.task_dict['accepted_values_a1_id__1__2__3'].upstream_list) == [
        'a__hourly__dependencies__group.wait__a1'
    ]

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_independent_domains_have_correct_dags(dags_independent_domains, run_airflow_tasks):
    dags = dags_independent_domains

    assert sorted(dags) == ['a__backfill', 'a__daily', 'b__backfill', 'b__daily']

    a = dags['a__daily']
    b = dags['b__daily']
    a_backfill = dags['a__backfill']

    assert a.dag_id == 'a__daily'
    assert sorted(a.task_ids) == ['a1', 'a2']
    assert node_ids(a.task_dict['a1'].upstream_list) == []
    assert node_ids(a.task_dict['a2'].upstream_list) == ['a1']

    assert b.dag_id == 'b__daily'
    assert sorted(b.task_ids) == ['b1', 'b2']
    assert node_ids(b.task_dict['b1'].upstream_list) == []
    assert node_ids(b.task_dict['b2'].upstream_list) == ['b1']

    assert a_backfill.dag_id == 'a__backfill'
    assert sorted(a_backfill.task_ids) == ['a1__bf', 'a2__bf', 'branch', 'do_nothing', 'start_work']
    assert node_ids(a_backfill.task_dict['branch'].upstream_list) == []
    assert node_ids(a_backfill.task_dict['branch'].downstream_list) == ['do_nothing', 'start_work']
    assert node_ids(a_backfill.task_dict['start_work'].downstream_list) == ['a1__bf']
    assert node_ids(a_backfill.task_dict['do_nothing'].downstream_list) == []

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_sequential_domains_have_correct_dags(dags_sequential_domains, run_airflow_tasks):
    dags = dags_sequential_domains

    assert sorted(dags) == ['a__backfill', 'a__daily', 'b__backfill', 'b__daily', 'c__backfill', 'c__daily']

    a = dags['a__daily']
    b = dags['b__daily']
    c = dags['c__daily']

    assert a.dag_id == 'a__daily'
    assert sorted(a.task_ids) == ['a1', 'a2']
    assert node_ids(a.task_dict['a1'].upstream_list) == []
    assert node_ids(a.task_dict['a2'].upstream_list) == ['a1']

    assert b.dag_id == 'b__daily'
    assert sorted(b.task_ids) == ['a__daily__dependencies__group.wait__a2', 'b1', 'b2']
    assert node_ids(b.task_dict['a__daily__dependencies__group.wait__a2'].upstream_list) == []
    assert node_ids(b.task_dict['b1'].upstream_list) == ['a__daily__dependencies__group.wait__a2']
    assert node_ids(b.task_dict['b2'].upstream_list) == ['b1']

    assert c.dag_id == 'c__daily'
    assert sorted(c.task_ids) == ['b__daily__dependencies__group.wait__b2', 'c1']
    assert node_ids(c.task_dict['b__daily__dependencies__group.wait__b2'].upstream_list) == []
    assert node_ids(c.task_dict['c1'].upstream_list) == ['b__daily__dependencies__group.wait__b2']

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_sequential_domains_have_correct_external_sensors(dags_sequential_domains, run_airflow_tasks):
    dags = dags_sequential_domains

    b_waits_sensor = dags['b__daily'].task_dict['a__daily__dependencies__group.wait__a2']

    assert b_waits_sensor.external_dag_id == 'a__daily'
    assert b_waits_sensor.external_task_ids == ['a2']

    c_waits_sensor = dags['c__daily'].task_dict['b__daily__dependencies__group.wait__b2']
    assert c_waits_sensor.external_dag_id == 'b__daily'
    assert c_waits_sensor.external_task_ids == ['b2']

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_sequential_tasks_in_one_domain_have_correct_dags(dags_sequential_tasks_in_one_domain, run_airflow_tasks):
    dags = dags_sequential_tasks_in_one_domain

    assert sorted(dags) == ['a__backfill', 'a__daily']

    a = dags['a__daily']

    assert a.dag_id == 'a__daily'
    assert sorted(a.task_ids) == ['a1', 'a2', 'a3']
    assert node_ids(a.task_dict['a1'].upstream_list) == []
    assert node_ids(a.task_dict['a2'].upstream_list) == ['a1']
    assert node_ids(a.task_dict['a3'].upstream_list) == ['a2']

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_two_domains_depend_on_another_have_correct_dags(dags_two_domains_depend_on_another, run_airflow_tasks):
    dags = dags_two_domains_depend_on_another

    assert sorted(dags) == ['a__backfill', 'a__daily', 'b__backfill', 'b__daily', 'c__backfill', 'c__daily']

    a = dags['a__daily']
    b = dags['b__daily']
    c = dags['c__daily']

    assert a.dag_id == 'a__daily'
    assert sorted(a.task_ids) == ['a1']
    assert node_ids(a.task_dict['a1'].upstream_list) == []

    assert b.dag_id == 'b__daily'
    assert sorted(b.task_ids) == ['a__daily__dependencies__group.wait__a1', 'b1']
    assert node_ids(b.task_dict['a__daily__dependencies__group.wait__a1'].upstream_list) == []
    assert node_ids(b.task_dict['b1'].upstream_list) == ['a__daily__dependencies__group.wait__a1']

    assert c.dag_id == 'c__daily'
    assert sorted(c.task_ids) == ['a__daily__dependencies__group.wait__a1', 'c1']
    assert node_ids(c.task_dict['a__daily__dependencies__group.wait__a1'].upstream_list) == []
    assert node_ids(c.task_dict['c1'].upstream_list) == ['a__daily__dependencies__group.wait__a1']

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_two_domains_depend_on_two_have_correct_dags(dags_two_domains_depend_on_two, run_airflow_tasks):
    dags = dags_two_domains_depend_on_two

    assert sorted(dags) == [
        'a__backfill',
        'a__daily',
        'b__backfill',
        'b__daily',
        'c__backfill',
        'c__daily',
        'd__backfill',
        'd__daily',
    ]

    a = dags['a__daily']
    b = dags['b__daily']
    c = dags['c__daily']
    d = dags['d__daily']

    assert a.dag_id == 'a__daily'
    assert sorted(a.task_ids) == ['a1']
    assert node_ids(a.task_dict['a1'].upstream_list) == []

    assert b.dag_id == 'b__daily'
    assert sorted(b.task_ids) == ['b1']
    assert node_ids(b.task_dict['b1'].upstream_list) == []

    assert c.dag_id == 'c__daily'
    assert sorted(c.task_ids) == [
        'a__daily__dependencies__group.wait__a1',
        'b__daily__dependencies__group.wait__b1',
        'c1',
    ]
    assert node_ids(c.task_dict['a__daily__dependencies__group.wait__a1'].upstream_list) == []
    assert node_ids(c.task_dict['b__daily__dependencies__group.wait__b1'].upstream_list) == []
    assert node_ids(c.task_dict['c1'].upstream_list) == [
        'a__daily__dependencies__group.wait__a1',
        'b__daily__dependencies__group.wait__b1',
    ]

    assert d.dag_id == 'd__daily'
    assert sorted(d.task_ids) == [
        'a__daily__dependencies__group.wait__a1',
        'b__daily__dependencies__group.wait__b1',
        'd1',
    ]
    assert node_ids(d.task_dict['a__daily__dependencies__group.wait__a1'].upstream_list) == []
    assert node_ids(d.task_dict['b__daily__dependencies__group.wait__b1'].upstream_list) == []
    assert node_ids(d.task_dict['d1'].upstream_list) == [
        'a__daily__dependencies__group.wait__a1',
        'b__daily__dependencies__group.wait__b1',
    ]

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_task_depends_on_two_within_same_domain_has_correct_dags(
    dags_task_depends_on_two_within_same_domain,
    run_airflow_tasks,
):
    dags = dags_task_depends_on_two_within_same_domain

    assert sorted(dags) == ['a__backfill', 'a__daily']

    a = dags['a__daily']

    assert a.dag_id == 'a__daily'
    assert sorted(a.task_ids) == ['a1', 'a2', 'a3']
    assert node_ids(a.task_dict['a1'].upstream_list) == []
    assert node_ids(a.task_dict['a2'].upstream_list) == []
    assert node_ids(a.task_dict['a3'].upstream_list) == ['a1', 'a2']

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_two_tasks_depend_on_one_have_correct_dags(dags_two_tasks_depend_on_one, run_airflow_tasks):
    dags = dags_two_tasks_depend_on_one

    assert sorted(dags) == ['a__backfill', 'a__daily']

    a = dags['a__daily']

    assert a.dag_id == 'a__daily'
    assert sorted(a.task_ids) == ['a1', 'a2', 'a3']
    assert node_ids(a.task_dict['a1'].upstream_list) == []
    assert node_ids(a.task_dict['a2'].upstream_list) == ['a1']
    assert node_ids(a.task_dict['a3'].upstream_list) == ['a1']

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_two_tasks_depend_on_two_have_correct_dags(dags_two_tasks_depend_on_two, run_airflow_tasks):
    dags = dags_two_tasks_depend_on_two

    assert sorted(dags) == ['a__backfill', 'a__daily']

    a = dags['a__daily']

    assert a.dag_id == 'a__daily'
    assert sorted(a.task_ids) == ['a1', 'a2', 'a3', 'a4']
    assert node_ids(a.task_dict['a1'].upstream_list) == []
    assert node_ids(a.task_dict['a2'].upstream_list) == []
    assert node_ids(a.task_dict['a3'].upstream_list) == ['a1', 'a2']
    assert node_ids(a.task_dict['a4'].upstream_list) == ['a1', 'a2']

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_domain_with_different_schedule_has_correct_dags(dags_domain_with_different_schedule, run_airflow_tasks):
    dags = dags_domain_with_different_schedule

    assert sorted(dags) == ['a__backfill', 'a__daily', 'a__hourly']

    a_daily = dags['a__daily']
    a_hourly = dags['a__hourly']

    assert a_daily.dag_id == 'a__daily'
    assert sorted(a_daily.task_ids) == ['a2', 'a__hourly__dependencies__group.wait__a1']
    assert node_ids(a_daily.task_dict['a__hourly__dependencies__group.wait__a1'].upstream_list) == []
    assert node_ids(a_daily.task_dict['a2'].upstream_list) == ['a__hourly__dependencies__group.wait__a1']

    assert a_hourly.dag_id == 'a__hourly'
    assert sorted(a_hourly.task_ids) == ['a1', 'a3', 'a__daily__dependencies__group.wait__a2']
    assert node_ids(a_hourly.task_dict['a1'].upstream_list) == []
    assert node_ids(a_hourly.task_dict['a__daily__dependencies__group.wait__a2'].upstream_list) == []
    assert node_ids(a_hourly.task_dict['a3'].upstream_list) == ['a__daily__dependencies__group.wait__a2']

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_domain_depends_on_another_with_multischeduling_has_correct_dags(
    dags_domain_depends_on_another_with_multischeduling, run_airflow_tasks
):
    dags = dags_domain_depends_on_another_with_multischeduling

    assert sorted(dags) == ['a__backfill', 'a__daily', 'a__hourly', 'b__backfill', 'b__hourly']

    a_hourly = dags['a__hourly']
    a_daily = dags['a__daily']
    b_hourly = dags['b__hourly']

    assert a_hourly.dag_id == 'a__hourly'
    assert sorted(a_hourly.task_ids) == ['a1']
    assert node_ids(a_hourly.task_dict['a1'].upstream_list) == []

    assert a_daily.dag_id == 'a__daily'
    assert sorted(a_daily.task_ids) == ['a2', 'a__hourly__dependencies__group.wait__a1']
    assert node_ids(a_daily.task_dict['a__hourly__dependencies__group.wait__a1'].upstream_list) == []
    assert node_ids(a_daily.task_dict['a2'].upstream_list) == ['a__hourly__dependencies__group.wait__a1']

    assert b_hourly.dag_id == 'b__hourly'
    assert sorted(b_hourly.task_ids) == ['a__daily__dependencies__group.wait__a2', 'b1']
    assert node_ids(b_hourly.task_dict['a__daily__dependencies__group.wait__a2'].upstream_list) == []
    assert node_ids(b_hourly.task_dict['b1'].upstream_list) == ['a__daily__dependencies__group.wait__a2']

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_domain_depends_on_another_with_test_has_correct_dags(
    dags_domain_depends_on_another_with_test, run_airflow_tasks
):
    dags = dags_domain_depends_on_another_with_test

    assert sorted(dags) == ['a__backfill', 'a__hourly', 'b__backfill', 'b__daily']

    a = dags['a__hourly']
    b = dags['b__daily']

    assert a.dag_id == 'a__hourly'
    assert sorted(a.task_ids) == ['a1__group.a1', 'a1__group.a1__end', 'a1__group.not_null_a1_id']
    assert node_ids(a.task_dict['a1__group.a1'].upstream_list) == []
    assert node_ids(a.task_dict['a1__group.not_null_a1_id'].upstream_list) == ['a1__group.a1']
    assert node_ids(a.task_dict['a1__group.a1__end'].upstream_list) == ['a1__group.not_null_a1_id']

    assert b.dag_id == 'b__daily'
    assert sorted(b.task_ids) == ['a__hourly__dependencies__group.wait__a1', 'b1']
    assert node_ids(b.task_dict['a__hourly__dependencies__group.wait__a1'].upstream_list) == []
    assert node_ids(b.task_dict['b1'].upstream_list) == ['a__hourly__dependencies__group.wait__a1']

    b_waits_sensor = b.task_dict['a__hourly__dependencies__group.wait__a1']

    assert b_waits_sensor.external_dag_id == 'a__hourly'
    assert b_waits_sensor.external_task_ids == ['a1__group.a1__end']

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_domain_with_enabled_disabled_models_has_correct_dags(dags_domain_w_enable_disable_models, run_airflow_tasks):
    dags = dags_domain_w_enable_disable_models

    assert sorted(dags) == ['a__backfill', 'a__daily']
    assert sorted(dags['a__daily'].task_ids) == [
        'a1__group.a1',
        'a1__group.a1_branch',
        'a2__group.a2',
        'a2__group.a2_branch',
    ]
    assert nodes_operator_names(dags['a__daily'].tasks) == {
        'a1__group.a1': 'DbtRun',
        'a1__group.a1_branch': 'DbtBranchOperator',
        'a2__group.a2': 'DbtRun',
        'a2__group.a2_branch': 'DbtBranchOperator',
    }

    assert node_ids(dags['a__daily'].task_dict['a1__group.a1_branch'].upstream_list) == []
    assert node_ids(dags['a__daily'].task_dict['a1__group.a1_branch'].downstream_list) == ['a1__group.a1']
    assert node_ids(dags['a__daily'].task_dict['a1__group.a1'].downstream_list) == []
    assert node_ids(dags['a__daily'].task_dict['a2__group.a2_branch'].upstream_list) == []
    assert node_ids(dags['a__daily'].task_dict['a2__group.a2_branch'].downstream_list) == ['a2__group.a2']
    assert node_ids(dags['a__daily'].task_dict['a2__group.a2'].downstream_list) == []

    assert sorted(dags['a__backfill'].task_ids) == [
        'a1__bf__group.a1__bf',
        'a1__bf__group.a1__bf_branch',
        'a2__bf__group.a2__bf',
        'a2__bf__group.a2__bf_branch',
        'branch',
        'do_nothing',
        'start_work',
    ]
    assert nodes_operator_names(dags['a__backfill'].tasks) == {
        'a1__bf__group.a1__bf': 'DbtRun',
        'a1__bf__group.a1__bf_branch': 'DbtBranchOperator',
        'a2__bf__group.a2__bf': 'DbtRun',
        'a2__bf__group.a2__bf_branch': 'DbtBranchOperator',
        'branch': 'BranchPythonOperator',
        'do_nothing': 'EmptyOperator',
        'start_work': 'EmptyOperator',
    }
    assert node_ids(dags['a__backfill'].task_dict['branch'].upstream_list) == []
    assert node_ids(dags['a__backfill'].task_dict['branch'].downstream_list) == ['do_nothing', 'start_work']
    assert node_ids(dags['a__backfill'].task_dict['start_work'].downstream_list) == [
        'a1__bf__group.a1__bf_branch',
        'a2__bf__group.a2__bf_branch',
    ]
    assert node_ids(dags['a__backfill'].task_dict['a1__bf__group.a1__bf_branch'].downstream_list) == [
        'a1__bf__group.a1__bf'
    ]
    assert node_ids(dags['a__backfill'].task_dict['a2__bf__group.a2__bf_branch'].downstream_list) == [
        'a2__bf__group.a2__bf'
    ]

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_dags_domain_w_source_freshness_has_correct_dags(dags_domain_w_source_freshness, run_airflow_tasks):
    dags = dags_domain_w_source_freshness

    assert sorted(dags) == ['a__backfill', 'a__daily']

    assert sorted(dags['a__daily'].task_ids) == [
        'a1__group.a1',
        'a1__group.wait_freshness___external_table_to_use_in_source__for__a1',
    ]
    assert nodes_operator_names(dags['a__daily'].tasks) == {
        'a1__group.a1': 'DbtRun',
        'a1__group.wait_freshness___external_table_to_use_in_source__for__a1': 'DbtSourceFreshnessSensor',
    }
    assert node_ids(dags['a__daily'].task_dict['a1__group.a1'].upstream_list) == [
        'a1__group.wait_freshness___external_table_to_use_in_source__for__a1'
    ]
    assert (
        node_ids(
            dags['a__daily']
            .task_dict['a1__group.wait_freshness___external_table_to_use_in_source__for__a1']
            .upstream_list
        )
        == []
    )

    assert sorted(dags['a__backfill'].task_ids) == [
        'a1__bf__group.a1__bf',
        'a1__bf__group.wait_freshness___external_table_to_use_in_source__for__a1__bf',
        'branch',
        'do_nothing',
        'start_work',
    ]
    assert node_ids(dags['a__backfill'].task_dict['branch'].upstream_list) == []
    assert node_ids(dags['a__backfill'].task_dict['branch'].downstream_list) == ['do_nothing', 'start_work']
    assert node_ids(dags['a__backfill'].task_dict['start_work'].downstream_list) == [
        'a1__bf__group.wait_freshness___external_table_to_use_in_source__for__a1__bf'
    ]
    assert node_ids(
        dags['a__backfill']
        .task_dict['a1__bf__group.wait_freshness___external_table_to_use_in_source__for__a1__bf']
        .downstream_list
    ) == ['a1__bf__group.a1__bf']
    assert node_ids(dags['a__backfill'].task_dict['a1__bf__group.a1__bf'].downstream_list) == []

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_dags_domain_w_task_in_kubernetes_has_correct_dags(dags_domain_w_task_in_kubernetes):
    dags = dags_domain_w_task_in_kubernetes
    assert sorted(dags) == ['a__backfill', 'a__daily', 'b__backfill', 'b__daily']

    assert sorted(dags['a__daily'].task_ids) == ['a1', 'b__daily__dependencies__group.wait__b1']
    assert nodes_operator_names(dags['a__daily'].tasks) == {
        'a1': 'DbtKubernetesPodOperator',
        'b__daily__dependencies__group.wait__b1': 'DbtExternalSensor',
    }
    assert node_ids(dags['a__daily'].task_dict['a1'].upstream_list) == ['b__daily__dependencies__group.wait__b1']
    assert node_ids(dags['a__daily'].task_dict['a1'].downstream_list) == []

    assert sorted(dags['b__daily'].task_ids) == ['b1']
    assert nodes_operator_names(dags['b__daily'].tasks) == {'b1': 'DbtRun'}


def test_dags_dags_domain_model_w_maintenance_has_correct_dags(dags_domain_model_w_maintenance, run_airflow_tasks):
    dags = dags_domain_model_w_maintenance
    assert sorted(dags) == ['a__backfill', 'a__daily', 'a__maintenance']

    assert sorted(dags['a__maintenance'].task_ids) == [
        'deduplicate_table__a__maintenance.dbt_deduplicate_table__a1',
        'optimize_table__a__maintenance.dbt_optimize_table__a1',
        'optimize_table__a__maintenance.dbt_optimize_table__a2',
        'persist_docs__a__maintenance.dbt_persist_docs__a1',
        'set_ttl_on_table__a__maintenance.dbt_set_ttl_on_table__a1',
        'set_ttl_on_table__a__maintenance.dbt_set_ttl_on_table__a2',
        'vacuum_table__a__maintenance.dbt_vacuum_table__a1',
        'vacuum_table__a__maintenance.dbt_vacuum_table__a2',
    ]

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags)


def test_task_with_tableau_integration_has_correct_dags(dags_task_with_tableau_integration, run_airflow_tasks):
    from dbt_af.parser.dbt_node_model import TableauRefreshResourceType, TableauRefreshTaskConfig

    dags = dags_task_with_tableau_integration
    assert sorted(dags) == ['a__backfill', 'a__hourly']

    assert sorted(dags['a__hourly'].task_ids) == ['a1__group.a1', 'a1__group.tableau_refresh__a1']
    assert nodes_operator_names(dags['a__hourly'].tasks) == {
        'a1__group.a1': 'DbtRun',
        'a1__group.tableau_refresh__a1': 'TableauExtractsRefreshOperator',
    }
    assert node_ids(dags['a__hourly'].task_dict['a1__group.a1'].upstream_list) == []
    assert node_ids(dags['a__hourly'].task_dict['a1__group.a1'].downstream_list) == ['a1__group.tableau_refresh__a1']

    tableau_refresh_tasks_observed = [
        task.dict()
        for task in dags['a__hourly'].task_dict['a1__group.tableau_refresh__a1'].op_kwargs['tableau_refresh_tasks']
    ]
    tableau_refresh_tasks_expected = [
        TableauRefreshTaskConfig(
            resource_name='workbook_name',
            project_name='project_name',
            resource_type=TableauRefreshResourceType.workbook,
        ).dict(),
        TableauRefreshTaskConfig(
            resource_name='datasource_name',
            project_name='project_name',
            resource_type=TableauRefreshResourceType.datasource,
        ).dict(),
    ]

    assert tableau_refresh_tasks_observed == tableau_refresh_tasks_expected

    if run_airflow_tasks:
        run_all_tasks_in_dag(dags, additional_expected_ti_states=(TaskInstanceState.SKIPPED,))
