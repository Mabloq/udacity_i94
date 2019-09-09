from datetime import datetime, timedelta
import os
from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators.dummy_operator import DummyOperator


from subdag import local_sub_dag
from data_quality_dag import quality_sub_dag
from tranform_parquet_sub_dag import transform_sub_dag
from copy_dag import copy_sub_dag
from helpers import SqlQueries, CopyFromParquet
import configparser
import logging

config = configparser.ConfigParser()
pathy = os.path.abspath(os.path.dirname(__file__))

logging.info("path: {}".format(os.path.join(pathy.split("airflow",1)[0],'airflow', 'immigration.cfg')))
config.read(os.path.join(pathy.split("airflow",1)[0],'airflow', 'immigration.cfg'))


PARENT_DAG_NAME = 'Immigration'
LOCAL_CHILD_DAG_NAME = 'StageToS3'
TRANSFORM_CHILD_DAG_NAME = 'TansformParquetToS3'
COPY_CHILD_DAG_NAME = 'CopyS3ToRedshift'
QUALITY_CHILD_DAG_NAME = "QualityChecks"

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
# /home/mabloq/Documents/workspace/sas_data
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

main_dag = DAG(
  dag_id=PARENT_DAG_NAME,
  default_args=default_args,
  description="Stage data to s3 then import to redshift",
  start_date=datetime(2019,7,27),
  schedule_interval='0 0 1 * *'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=main_dag)

drop_tables = PostgresOperator(
    task_id="drop_tables",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.drop_oltp_tables
)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_oltp_tables
)


# sub_dag_local = SubDagOperator(
#   subdag=local_sub_dag(PARENT_DAG_NAME, LOCAL_CHILD_DAG_NAME,main_dag.start_date,
#                  main_dag.schedule_interval),
#   task_id=LOCAL_CHILD_DAG_NAME,
#   dag=main_dag,
# )

# sub_dag_transform = SubDagOperator(
#   subdag=transform_sub_dag(PARENT_DAG_NAME, TRANSFORM_CHILD_DAG_NAME, main_dag.start_date,
#                  main_dag.schedule_interval),
#   task_id=TRANSFORM_CHILD_DAG_NAME,
#   dag=main_dag,
# )
# #
sub_dag_copy = SubDagOperator(
  subdag=copy_sub_dag(PARENT_DAG_NAME, COPY_CHILD_DAG_NAME, main_dag.start_date,
                 main_dag.schedule_interval),
  task_id=COPY_CHILD_DAG_NAME,
  dag=main_dag,
)

sub_dag_quality = SubDagOperator(
  subdag=quality_sub_dag(PARENT_DAG_NAME, QUALITY_CHILD_DAG_NAME, main_dag.start_date,
                 main_dag.schedule_interval),
  task_id=QUALITY_CHILD_DAG_NAME,
  dag=main_dag,
)

#
# start_operator >> drop_tables >> create_tables >> sub_dag_transform >> sub_dag_copy >> sub_dag_quality
start_operator >> drop_tables >> create_tables >> sub_dag_copy >> sub_dag_quality