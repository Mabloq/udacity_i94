
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.postgres_operator import PostgresOperator
import configparser
from helpers import CopyFromParquet

config = configparser.ConfigParser()
config.read('/home/mabloq/airflow/immigration.cfg')


def copy_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    copy_ports = PostgresOperator(
        dag=dag,
        task_id="ports",
        postgres_conn_id="redshift",
        sql=CopyFromParquet.ports_to_redshift
    )
    copy_country = PostgresOperator(
        dag=dag,
        task_id="countries",
        postgres_conn_id="redshift",
        sql=CopyFromParquet.country_to_redshift
    )

    copy_visa = PostgresOperator(
        dag=dag,
        task_id="visa",
        postgres_conn_id="redshift",
        sql=CopyFromParquet.visa_to_redshift
    )

    copy_demographics = PostgresOperator(
        dag=dag,
        task_id="demographics",
        postgres_conn_id="redshift",
        sql=CopyFromParquet.demographics_to_redshift
    )
    copy_i94 = PostgresOperator(
        task_id="i94",
        dag=dag,
        postgres_conn_id="redshift",
        sql=CopyFromParquet.i94_to_redshift
    )




    return dag
