import datetime
import json
import re

import isodate
import requests
from pyspark.sql.functions import udf, when, col, lit, to_date
from pyspark.sql.types import LongType

from lib.logger import Log4j

__author__ = 'Sumeet Gupta'
logger: Log4j = Log4j(None)


def process(spark, input_file, output_path):
    # this method has the spark transformations required to solve the problem  statement.
    global logger
    logger = Log4j(spark)  # setting  global logger

    if input_file is not None:
        input_df = spark.read.json(str(input_file))
    else:
        # reading data from API. Not recommended in Production environment to read the file from REST
        json_response = requests.get("https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json")
        input_df = spark.createDataFrame(data=[json.loads(line) for line in json_response.iter_lines()])

    # registering UDFs
    udf_get_seconds_from_duration = udf(get_seconds_from_duration, LongType())
    udf_get_duration_from_seconds = udf(get_duration_from_seconds)

    # cleaning the input data by converting durations to seconds and date field to DateType
    clean_df = input_df.withColumn("prepSeconds", udf_get_seconds_from_duration("prepTime")). \
        withColumn("cookSeconds", udf_get_seconds_from_duration("cookTime")). \
        withColumn("datePublished", (to_date(col("datePublished")))).persist()  # persist is not needed here
    # as only single call is happening

    # Task-2
    output_df = clean_df \
        .where("cookSeconds>0 and prepSeconds>0") \
        .selectExpr("cookSeconds + prepSeconds as totalSeconds") \
        .withColumn("difficulty", when(col("totalSeconds") < 60 * 30, lit("easy")) \
                    .otherwise(when(col("totalSeconds") > 60 * 30, lit("hard")) \
                               .otherwise(lit("medium")))) \
        .groupBy("difficulty").avg() \
        .withColumnRenamed("avg(totalSeconds)", "avg_total_cooking_seconds") \
        .withColumn("avg_total_cooking_time", udf_get_duration_from_seconds("avg_total_cooking_seconds")) \
        .drop("avg_total_cooking_seconds").persist()
    # output_df.coalesce(1).write.mode("overwrite").csv("../output")
    # to_pandas not recommended when using spark if data size is large
    # provided relative path.also not recommended
    output_df.coalesce(1).toPandas().to_csv(output_path, index=False)


def get_seconds_from_duration(dur):
    # method that takes String ISO8601 duration format string and returns the Long value of seconds passed
    try:
        if re.fullmatch('^P(?!$)(\d+Y)?(\d+M)?(\d+W)?(\d+D)?(T(?=\d+[HMS])(\d+H)?(\d+M)?(\d+S)?)?$',
                        str(dur)) is None:
            # checking if the duration provided is on correct ISO 8601 durations format
            logger.error(message="Invalid Duration." + str(dur))
            return int(-10)
            # return negative if not correct. in our use case the duration can not be negative
        else:
            dur = isodate.parse_duration(str(dur))
            return int(dur.total_seconds())
    except Exception as inst:
        logger.error(message=str(inst))
        return int(-10)  # return negative number in case of error


def get_duration_from_seconds(seconds):
    # method that takes Long value of seconds passed and returns  the String ISO8601 duration format string
    try:
        if int(seconds) < 0:
            logger.error("Time Duration can not be negative" + str(seconds))
            return ""  # return empty string when error
        else:
            return isodate.duration_isoformat(datetime.timedelta(seconds=int(seconds)))
    except Exception as inst:
        logger.error(message=str(inst))
        return ""  # return empty string when error
