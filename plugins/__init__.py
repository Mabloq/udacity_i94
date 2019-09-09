from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers


# Defining the plugin class
class ImmigrationPlugin(AirflowPlugin):
    name = "Immigration__Plugin"
    operators = [
        # operators.StageToRedshiftOperator,
        # operators.LoadFactOperator,
        operators.LocalToS3Operator,
        operators.DataQualityOperator,
        operators.SparkS3ParquetTransformOperator
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.CopyFromParquet
    ]
