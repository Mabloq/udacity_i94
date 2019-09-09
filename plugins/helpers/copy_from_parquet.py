import configparser
import os
import logging

pathy = os.path.abspath(os.path.dirname(__file__))
config = configparser.ConfigParser()
logging.info("path: {}".format(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'airflow','immigration.cfg')))
config.read(os.path.join(pathy.split("airflow",1)[0],'airflow', 'immigration.cfg'))

class CopyFromParquet:
    i94_to_redshift = ("""
    COPY i94
    FROM 's3://mabloq-udacity/parquet/i94/'
    IAM_ROLE {}
    FORMAT AS PARQUET;
    """).format(config['IAM_ROLE']['ARN'])

    ports_to_redshift = ("""
    COPY ports
    FROM 's3://mabloq-udacity/parquet/ports'
    IAM_ROLE {}
    FORMAT AS PARQUET;
    """).format(config['IAM_ROLE']['ARN'])

    country_to_redshift = ("""
    COPY countries
    FROM 's3://mabloq-udacity/parquet/countries'
    IAM_ROLE {}
    FORMAT AS PARQUET;
    """).format(config['IAM_ROLE']['ARN'])

    visa_to_redshift = ("""
    COPY visa_codes
    FROM 's3://mabloq-udacity/parquet/visas'
    IAM_ROLE {}
    FORMAT AS PARQUET;
    """).format(config['IAM_ROLE']['ARN'])

    demographics_to_redshift = ("""
    COPY demographics
    FROM 's3://mabloq-udacity/parquet/demographics'
    IAM_ROLE {}
    FORMAT AS PARQUET;
    """).format(config['IAM_ROLE']['ARN'])
