from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import glob
import logging


class LocalToS3Operator(BaseOperator):
    """
    If you have data files local to your airflow instance, this is the Operator that will get
    that data into s3

    :param aws_id: an airflow connection id to  aws
    :type aws_id: str
    :param bucket: s3 bucket name
    :type bucket: str
    :param s3key: key that will point to file
    :type s3key: str
    :param src: path to filename
    :type src: str
    :param dir: indicates if file path is a directory
    :type dir: bool
    :param verify: check ssl or not check ssl
    :type verify: str
    """
    ui_color = '#c7c7c7'

    @apply_defaults
    def __init__(self,
                 aws_id,
                 bucket,
                 s3key,
                 src,
                 dir=False,
                 verify=False,
                 *args, **kwargs):

        super(LocalToS3Operator, self).__init__(*args, **kwargs)
        self.aws_id = aws_id
        self.bucket = bucket
        self.s3key = s3key
        self.src = src
        self.dir = dir
        self.verify = verify

    def extract_file_name(self):
        pass


    def gen_file_paths(self,directory):
        files_paths = []

        for root, dirs, files in os.walk(directory):
            files = glob.glob(os.path.join(root, '*'))
            for f in files:
                files_paths.append(os.path.abspath(f))

        for f in files_paths:
            logging.info("file added {} in /{}".format(f, directory))

        return files_paths

    def execute(self, context):
        logging.info('connecting to {} bucket using {} and putting in key: {}'.format(self.bucket,self.aws_id,self.s3key))
        hook = S3Hook(aws_conn_id=self.aws_id, verify=self.verify)

        logging.info('hOOK IS WORKING')
        if self.dir:
            logging.info('hook is dir')
            files = self.gen_file_paths(self.src)

            for f in files:

                head, tail = os.path.split(f)


                hook.load_file(
                    filename=f,
                    key=self.s3key+tail,
                    bucket_name=self.bucket,
                    replace=True
                )
            return

        logging.info('file {} is logged'.format(self.src))
        hook.load_file(
            filename=self.src,
            key=self.s3key,
            bucket_name=self.bucket,
            replace=True
        )


