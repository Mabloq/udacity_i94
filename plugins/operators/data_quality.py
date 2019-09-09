from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class DataQualityOperator(BaseOperator):
    """
    checks that that our tables are not empty and if null_check_schema is provide
    this operator will also check columns in our table for null values

    :param redshift_conn_id: an airflow connection id to a redshift database
    :type redshift_conn_id: str
    :param tables: list of redshift tables names strings
    :type tables: list
    :param null_check_schema: a dictionary with table names as keys and list of columns to check for nulls as values
    :type null_check_schema: dict
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables,
                 null_check_schema=None,
                 # date_check=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.null_check_schema = null_check_schema
        # self.date_check = date_check

    def check_empty(self, conn, table):
        """
        Checks if a table is empty and throws error if the tbale is empty or logs successful
        flush and fill and the number of records if the table is not empty.

        :param conn: instatiated PostgresHook live connection
        :type conn: PostgresHook
        :param table: name of table to check if empty
        :type table: str
        """

        logging.info(f"Checking if {table} is empty")
        records = conn.get_records(f"SELECT COUNT(*) FROM {table}")

        if records is None or len(records[0]) < 1:
            raise ValueError(f"No records present in destination table: {table}")

        logging.info(f"Data Quality Check passed with {records[0][0]} records")

    def check_nulls(self, conn, table, col):
        """
        Checks if a table has null value in a set of columns and throws error if null values found in col or logs successful
        flush and fill and the number of records in each col that are null i.e. 0.

        :param conn: instatiated PostgresHook live connection
        :type conn: PostgresHook
        :param table: name of table to check if empty
        :type table: str
        :param col: column in table to check null values for
        :type col: str
        """
        logging.info(f"Checking if table:{table} col:{col} has null value")
        redshift_con = PostgresHook(conn)
        records = conn.get_records(f"select count(*) from {table} where {col} is null;")


        if records[0][0] != 0:
            raise ValueError(f"There is {records[0][0]} rows with null values in {col} column of {table} table")

        logging.info(f"Data Quality Null Check on {table}:{col} passed with {records[0][0]} null records")

    # def check_date(self, conn, table, col):
    #     """
    #        Checks if a table has date type in a set of columns and throws error if non date type values found in col or logs successful
    #        flush and fill and the number of records in each col that are null i.e. 0.
    #
    #        :param conn: instatiated PostgresHook live connection
    #        :type conn: PostgresHook
    #        :param table: name of table to check if empty
    #        :type table: str
    #        :param col: column in table to check null values for
    #        :type col: str
    #        """
    #     logging.info(f"Checking if table:{table} col:{col} has all date types")
    #     redshift_con = PostgresHook(conn)
    #     records = conn.get_records(f"select count(*) from {table} where {col} is null;")
    #     logging.info('1: ', records[0][0])
    #
    #     if records[0][0] != 0:
    #         raise ValueError(f"There is {records[0][0]} rows with null values in {col} column of {table} table")
    #
    #     logging.info(f"Data Quality Date Check on {table}:{col} passed with {records[0][0]} null records")

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for t in self.tables:
            self.check_empty(redshift, t)

        if self.null_check_schema is not None:
            for t in self.tables:
                for col in self.null_check_schema[t]:
                    self.check_nulls(redshift, t, col)
