
from tempfile import NamedTemporaryFile, TemporaryDirectory, mkdtemp

import subprocess
import sys
from typing import Union, List
import os
import shutil
import glob
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from pyspark.sql import SparkSession
from pyspark import SparkContext
import configparser
config = configparser.ConfigParser()
pathy = os.path.abspath(os.path.dirname(__file__))

logging.info("path: {}".format(os.path.join(pathy.split("airflow",1)[0],'airflow', 'immigration.cfg')))
config.read(os.path.join(pathy.split("airflow",1)[0],'airflow', 'immigration.cfg'))

aws_access = config['S3']['ACCESS']
aws_secret = config['S3']['SECRET']

class SparkS3ParquetTransformOperator(BaseOperator):
    """
    Uses Local Spark to read from s3_path into local spark data frame. The provided transform script
    will make changes to the dataframe and the changes will be written in parquet format to s3_dest path

    This Operator requires that the PC that runs airflow to have spark installed in standalone mode
    and hadoop installed and spark must be configured to share jars with spark please see for instruction:
        https://medium.com/@sivachaitanya/install-apache-spark-pyspark-standalone-mode-70d3c2dd8924


    :param source_s3: The s3 path to be retrieved from (templatable).
    :type source_s3: str
    :param dest_s3: The s3 path to be written to (templateable.
    :type dest_s3: str
    :param transform_script: the transformation python callable
    :type transform_script: function
    :param join_s3: path to s3 file to joing with in tranform script
    :type join_s3: str
    :param replace: Replace dest S3 pathif it already exists
    :type replace: bool
    """

    template_fields = ('source_s3', 'dest_s3')
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
            self,
            source_s3: str,
            dest_s3: str,
            transform_script = None,
            replace: bool = False,
            join_s3: List[str] = None,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.source_s3 = source_s3
        self.dest_s3 = dest_s3
        self.join_s3 = join_s3
        self.replace = replace
        self.transform_script = transform_script


    def execute(self, context):
        if self.transform_script is None:
            raise AirflowException(
                "Transform_script  must be specified")

        spark = SparkSession.builder \
            .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key",aws_access) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
            .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.parquet.mergeSchema", "false") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .config("spark.speculation", "false") \
            .enableHiveSupport().getOrCreate()

        if self.transform_script is not None:
            try:
                if self.join_s3:

                    self.transform_script(spark,self.source_s3, self.dest_s3, self.join_s3)
                else:

                    self.transform_script(spark,self.source_s3, self.dest_s3)
            except Exception as err:

                self.log.info("Tranform fail: ", err)
                raise AirflowException(
                    "Sorry, transform script from {} to {} has failed".format(self.source_s3,self.dest_s3))

        spark.stop()



