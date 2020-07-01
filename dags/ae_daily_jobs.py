from airflow import DAG
from datetime import datetime, timedelta
from pendulum import timezone

from airflow.contrib.kubernetes.pod import Port
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

TZ = timezone("America/Chicago")
MAILTO = ['qin@simpli.fi']

PORT = Port('http', 80)

default_args = {
    'owner': 'QW',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 17, 1, 00, 0, tzinfo=TZ),
    'email': MAILTO,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'task_concurrency': 1,
    'max_active_runs': 1,
    'pool': 'default_pool'
    # 'queue': 'bash_queue',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    dag_id='ae_daily_modeling',
    description='Airflow DAG for three daily modeling processes of Audience Expansion',
    default_args=default_args,
    schedule_interval='0 3 * * *',
    catchup=False
)

start = DummyOperator(task_id='Job_Start', dag=dag)

old_keywords = KubernetesPodOperator(
    namespace='default',
    image="us.icr.io/sifi_ds/audience_expansion:latest",
    cmds=["/bin/sh", "-c"],
    arguments=["python3 -u /audience_development/keyword_recommendation/kwd_old_kwd.py"],
    image_pull_policy='Always',
    labels={"environment": "production", "track": "daily"},
    ports=[PORT],
    name="old_keywords",
    task_id="old_kwd",
    in_cluster=True,
    is_delete_operator_pod=False,
    get_logs=True,
    retries=1,
    dag=dag
)

clustering = KubernetesPodOperator(
    namespace='default',
    image="us.icr.io/sifi_ds/audience_expansion:latest",
    cmds=["/bin/sh", "-c"],
    arguments=["python3 /audience_development/keyword_recommendation/kwd_cluster.py"],
    labels={"environment": "production", "track": "daily"},
    ports=[PORT],
    name="clustering",
    task_id="kw_cluster",
    in_cluster=True,
    is_delete_operator_pod=False,
    get_logs=False,
    retries=1,
    dag=dag
)

recommendation = KubernetesPodOperator(
    namespace='default',
    image="us.icr.io/sifi_ds/audience_expansion:latest",
    cmds=["/bin/sh", "-c"],
    arguments=["python3 /audience_development/keyword_recommendation/kwd_recommend.py"],
    labels={"environment": "production", "track": "daily"},
    ports=[PORT],
    name="recommendation",
    task_id="kw_recommend",
    in_cluster=True,
    is_delete_operator_pod=False,
    get_logs=False,
    retries=1,
    dag=dag
)

start >> old_keywords >> clustering >> recommendation
