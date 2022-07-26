from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from contrib.operators.finish_operator import FinishOperator
from contrib.operators.start_operator import StartOperator
from contrib.utils.dag_config import DAGConfig
from contrib.utils.dag_temp_dir import DAGTempDir
from contrib.operators.query_monitoring_operator import QueryMonitoringOperator
from airflow import configuration

config = DAGConfig(owner=' ',
                   job_config={'gc_conn_id': Variable.get('gcp_service_account'),
                               'gcp_project_id': Variable.get('gcp_project_id')
                               },
                   var_dag_job_config='conf_dag_query_monitoring')

default_args = config.default_args
job_config = config.job_config
start_date = datetime(2022, 5, 26, 13)

with DAG(dag_id='dag_query_monitoring_v1',
         schedule_interval='0 * * * *',
         start_date=start_date,
         default_args=default_args) as dag:
    temp_dir = DAGTempDir(dag_id=dag.dag_id, templated=True)

    # Start
    task_start_dag = StartOperator(
        task_id='start_dag_query_monitoring'
    )

    #Query Monitoring
    task_query_monitoring = QueryMonitoringOperator(
        task_id='query_monitoring',
        bigquery_conn_id=job_config['gc_conn_id'],
        list_projects=[project for project in job_config['list_projects']],
        minute_filter=job_config['minute_filter'],
        substring=job_config['substring']
    )

    # Finish
    task_finish_dag = FinishOperator(
        task_id='finish_dag_query_monitoring'
    )

    task_start_dag >> \
        task_query_monitoring >> \
        task_finish_dag