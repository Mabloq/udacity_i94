import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import split as spark_split, udf, to_date
from pyspark.sql.functions import concat, col, lit, upper, trim
from pyspark.sql.types import StructType, StructField as Fld, DecimalType as Dec, DateType, StringType as Str, IntegerType as Int, ShortType as Shrt
from datetime import datetime
from datetime import timedelta
pd.set_option('display.max_columns', 500)
import logging



class ParquetTransforms:
    @staticmethod
    def i94_to_parquet(spark,src, dest):


        @udf(DateType())
        def date_add_(start, add):
            if type(start) is not datetime:
                date = datetime.strptime(start, "%Y-%m-%d")
                if add is None:
                    return date

                return date + timedelta(add)

            if add is None:
                return start

            return start + timedelta(add)


        i94_df = spark.read.parquet(src)


        i94 = i94_df.selectExpr("cicid as cic_id", "i94port as port_id", "visatype as visa_id",
                                "i94cit as cit_id", "i94res as res_id", "i94yr as year", "i94mon as month",
                                "i94bir as age", "gender as gender", "arrdate as arrival_date", "depdate as depart_date",
                                "dtadfile as date_begin", "dtaddto as date_end")

        i94 = i94.withColumn('cic_id', col('cic_id').cast(Int()))
        i94 = i94.withColumn('cit_id', col('cit_id').cast(Shrt()))
        i94 = i94.withColumn('res_id', col('res_id').cast(Shrt()))
        i94 = i94.withColumn('year', col('year').cast(Shrt()))
        i94 = i94.withColumn('month', col('month').cast(Shrt()))
        i94 = i94.withColumn('age', col('age').cast(Shrt()))

        i94 = i94.withColumn('date_begin', to_date('date_begin', 'yyyyMMdd'))
        i94 = i94.withColumn('date_end', to_date('date_end', 'MMddyyyy'))

        i94 = i94.withColumn('depart_date', col('depart_date').cast(Int()))
        i94 = i94.withColumn('sas_date', lit("1960-01-01"))
        i94 = i94.withColumn('arrival_date', date_add_('sas_date', 'depart_date'))

        i94 = i94.withColumn('arrival_date', col('arrival_date').cast(Int()))
        i94 = i94.withColumn('arrival_date', date_add_('sas_date', 'arrival_date'))
        i94 = i94.drop('sas_date')

        i94.write.mode('overwrite').parquet(dest)


    @staticmethod
    def ports_to_parquet(spark,src, dest):

        @udf(Str())
        def state_null(col, state):
            if state is None:
                if col.startswith("No PORT"):
                    return "NPRT"
                elif col.startswith("Collapsed"):
                    return "CPRT"
                elif "UNKOWN" in col or "UNIDENTIFIED" in col:
                    return "UNKWN"
                elif "WASHINGTON DC" in col:
                    return "DC"
                elif "MARIPOSA AZ" in col:
                    return "AZ"
            else:
                return state

        iport_df = spark.read.csv(src, sep="=", header=True,
                                  ignoreLeadingWhiteSpace=True,
                                  ignoreTrailingWhiteSpace=True, quote="'")

        # split column in 2
        split_col = spark_split(iport_df['name'], ',')
        iport_df = iport_df.withColumn('city', split_col.getItem(0))
        iport_df = iport_df.withColumn('state', split_col.getItem(1))

        # Fill nulls in state column
        iport_df = iport_df.withColumn('state', state_null('name', 'state'))

        #Drop any nulls that dont get filled with our custom function
        iport_df = iport_df.filter(iport_df.state.isNotNull())
        iport_df.write.mode('overwrite').parquet(dest)


    @staticmethod
    def country_to_parquet(spark,src,dest):

        countries_df = spark.read.csv(src, sep="=", header=True,
                                      ignoreLeadingWhiteSpace=True,
                                      ignoreTrailingWhiteSpace=True, quote="'")

        countries_df = countries_df.withColumn('country_id',col('country_id').cast(Shrt()))
        countries_df.write.mode('overwrite').parquet(dest)


    @staticmethod
    def visa_to_parquet(spark, src, dest):

        visa_df = spark.read.csv(src, schema="col0 STRING, col1 STRING", header=True)


        visa_df.write.mode('overwrite').parquet(dest)



    @staticmethod
    def demographics_to_parquet(spark, src,dest, joins):

        @udf('string')
        def gen_demo_id(race):
            if race == "Black or African-American":
                return "BAA"
            elif race == "Hispanic or Latino":
                return "HL"
            elif race == "White":
                return "W"
            elif race == "Asian":
                return "A"
            elif race == "American Indian and Alaska Native":
                return "AI"
            else:
                return "O"

        cSchema = StructType([
            Fld("demographics_id", Str(), False),
            Fld("port_id", Str(), False),
            Fld("city", Str(), True),
            Fld("state", Str(), True),
            Fld("median_age", Dec(4,1), True),
            Fld("male_population", Int(), True),
            Fld("female_population", Int(), True),
            Fld("total_population", Int(), True),
            Fld("avg_household_size", Dec(3, 2), True),
            Fld("foreign_born", Int(), True),
            Fld("race", Str(), True),
            Fld("race_code", Str(), True)])

        demographics_df = spark.read.csv(src, sep=";", header=True)

        port_df = spark.read.parquet(joins[0])

        demographics_df = demographics_df.withColumn('race_code', gen_demo_id("Race"))

        joined_df = demographics_df.join(port_df, [upper(demographics_df['City']) == upper(port_df['city']),
                                                   trim(demographics_df['State Code']) == trim(port_df['state'])])

        joined_df = joined_df.select(demographics_df["*"], port_df['port_id'])

        joined_df = joined_df.withColumn('demographics_id', concat(col("port_id"), lit("-"), col("race_code")))

        joined_df = joined_df.selectExpr("demographics_id", "port_id", "city", "state",
                                         "`Median Age` as median_age", "`Male Population` as male_population",
                                         "`Female Population` as female_population",
                                         "`Total Population` as total_population",
                                         "`Average Household Size` as avg_household_size",
                                         "`Foreign-born` as foreign_born", "`Race` as race",
                                         "race_code")

        #apply typing for easy copy into redshift later

        joined_df = joined_df.withColumn("demographics_id", col("demographics_id").cast(Str()))
        joined_df = joined_df.withColumn("port_id", col("port_id").cast(Str()))
        joined_df = joined_df.withColumn("city", col("city").cast(Str()))
        joined_df = joined_df.withColumn("state", col("state").cast(Str()))
        joined_df = joined_df.withColumn("median_age", col("median_age").cast(Dec(4,1)))
        joined_df = joined_df.withColumn("male_population", col("male_population").cast(Int()))
        joined_df = joined_df.withColumn("female_population",col("female_population").cast(Int()))
        joined_df = joined_df.withColumn("total_population", col("total_population").cast(Int()))
        joined_df = joined_df.withColumn("avg_household_size", col("avg_household_size").cast(Dec(3,2)))
        joined_df = joined_df.withColumn("foreign_born", col("foreign_born").cast(Int()))
        joined_df = joined_df.withColumn("race", col("race").cast(Str()))
        joined_df = joined_df.withColumn("race_code", col("race_code").cast(Str()))

        #fill nulls with 0
        joined_df = joined_df.fillna(0)
        joined_df.write.mode('overwrite').parquet(dest)

