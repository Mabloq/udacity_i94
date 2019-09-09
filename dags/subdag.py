from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import DataQualityOperator, LocalToS3Operator

import configparser


config = configparser.ConfigParser()
config.read('/home/mabloq/airflow/immigration.cfg')


def local_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    dummy_operator = DummyOperator(
        task_id='dummy_task',
        dag=dag,
    )

    stage_to_s3_airlines = LocalToS3Operator(
        task_id="stage_s3_airlines",
        dag=dag,
        aws_id="s3con",
        bucket=config['S3']['BUCKET'],
        src=config['LOCAL']['AIR_LINES'],
        s3key='airlines.csv'
    )

    stage_to_s3_countries = LocalToS3Operator(
        task_id="stage_s3_countries",
        dag=dag,
        aws_id="s3con",
        bucket=config['S3']['BUCKET'],
        src=config['LOCAL']['COUNTRIES'],
        s3key='i_country.txt'
    )

    stage_to_s3_ports = LocalToS3Operator(
        task_id="stage_s3_ports",
        dag=dag,
        aws_id="s3con",
        bucket=config['S3']['BUCKET'],
        src=config['LOCAL']['PORTS'],
        s3key='i_port.txt'
    )

    stage_to_s3_demographics = LocalToS3Operator(
        task_id="stage_s3_demographics",
        dag=dag,
        aws_id="s3con",
        bucket=config['S3']['BUCKET'],
        src=config['LOCAL']['DEMOGRAPHICS'],
        s3key="us-cities-demographics.csv"
    )

    stage_to_s3_visa_codes = LocalToS3Operator(
        task_id="stage_s3_visa_codes",
        dag=dag,
        aws_id="s3con",
        bucket=config['S3']['BUCKET'],
        src=config['LOCAL']['VISA_CODES'],
        s3key="visa_types.txt"
    )

    stage_to_s3_i94 = LocalToS3Operator(
        task_id="stage_s3_i94",
        dag=dag,
        aws_id="s3con",
        bucket=config['S3']['BUCKET'],
        src=config['LOCAL']['I94'],
        s3key="i94/",
        dir=True
    )

    return dag
