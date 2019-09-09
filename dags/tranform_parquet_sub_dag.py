
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import SparkS3ParquetTransformOperator
import configparser
from helpers import SqlQueries,ParquetTransforms

config = configparser.ConfigParser()
config.read('/home/mabloq/airflow/immigration.cfg')


def transform_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    port_to_parquet = SparkS3ParquetTransformOperator(
        dag=dag,
        task_id="ports",
        transform_script=ParquetTransforms.ports_to_parquet,
        source_s3="s3a://mabloq-udacity/i_port.txt",
        dest_s3="s3a://mabloq-udacity/parquet/ports/",
        replace=True
    )

    country_to_parquet = SparkS3ParquetTransformOperator(
        dag=dag,
        task_id="countries",
        transform_script=ParquetTransforms.country_to_parquet,
        source_s3="s3a://mabloq-udacity/i_country.txt",
        dest_s3="s3a://mabloq-udacity/parquet/countries/",
        replace=True
    )
    #
    visa_to_parquet = SparkS3ParquetTransformOperator(
        dag=dag,
        task_id="visas",
        transform_script=ParquetTransforms.visa_to_parquet,
        source_s3="s3a://mabloq-udacity/visa_types.txt",
        dest_s3="s3a://mabloq-udacity/parquet/visas/",
        replace=True
    )

    demographics_to_parquet = SparkS3ParquetTransformOperator(
        dag=dag,
        task_id="demographics",
        transform_script=ParquetTransforms.demographics_to_parquet,
        source_s3="s3a://mabloq-udacity/us-cities-demographics.csv",
        dest_s3="s3a://mabloq-udacity/parquet/demographics/",
        replace=True,
        join_s3=['s3a://mabloq-udacity/parquet/ports']
    )

    i94_to_parquet = SparkS3ParquetTransformOperator(
        dag=dag,
        task_id="i94",
        transform_script=ParquetTransforms.i94_to_parquet,
        source_s3="s3a://mabloq-udacity/i94/",
        dest_s3="s3a://mabloq-udacity/parquet/i94/",
        replace=True,
    )
    #
    port_to_parquet >> country_to_parquet >> visa_to_parquet >> demographics_to_parquet >> i94_to_parquet

    return dag
