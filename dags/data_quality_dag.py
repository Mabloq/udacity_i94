
from airflow import DAG
from airflow.operators import DataQualityOperator
import os
import configparser
import logging

pathy = os.path.abspath(os.path.dirname(__file__))

logging.info("path: {}".format(os.path.join(pathy.split("airflow",1)[0],'airflow', 'immigration.cfg')))
config = configparser.ConfigParser()
config.read(os.path.join(pathy.split("airflow",1)[0],'airflow', 'immigration.cfg'))

def quality_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    port_quality_checks = DataQualityOperator(
        task_id='ports_quality_checks',
        dag=dag,
        redshift_conn_id="redshift",
        tables=['ports'],
        null_check_schema={'ports': ['port_id', 'state']}
    )

    demo_quality_check = DataQualityOperator(
        task_id='demographics_quality_checks',
        dag=dag,
        redshift_conn_id="redshift",
        tables=['demographics'],
        null_check_schema={'demographics': ['state', 'demographics_id']},

    )
    i94_quality_check = DataQualityOperator(
        task_id='i94_quality_checks',
        dag=dag,
        redshift_conn_id="redshift",
        tables=['i94'],
        null_check_schema={'i94': ['cic_id', 'visa_id', 'port_id']},

    )
    #

    return dag
