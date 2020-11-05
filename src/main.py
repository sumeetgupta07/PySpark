#!/usr/bin/python
import argparse
import os

import sys
import time
import yaml
from pyspark import SparkConf
from pyspark.sql import SparkSession

from lib.logger import Log4j

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')


def get_spark(config_path):
    # getting Spark Configurations from YAML config file.The path can also be taken as argument
    with open(config_path) as file:
        conf: dict = yaml.load(file)
        spark_conf = SparkConf().setAll(conf.items())
        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        return spark


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--config-file', type=str, dest='config_file', required=True,
                        help="The spark job config file")
    parser.add_argument('--input-file', type=str, dest='input_file',  # should be required=True for production
                        help="Input file path")
    parser.add_argument('--output-path', type=str, dest='output_path',  # should be required=True for production
                        help="Output file path")

    args = parser.parse_args()

    print("Called with arguments: %s" % args)

    config_file = args.config_file
    spark = get_spark(config_file)
    logger = Log4j(spark)
    logger.info("starting with config file-" + config_file)
    # job_module = importlib.import_module('jobs.recipe')

    if args.input_file:
        input_file = args.input_file
        logger.info("will try to read from input file:" + input_file)
    else:
        logger.info("input file not provided")
        input_file = None

    if args.output_path:
        output_path = args.output_path
        logger.info("will try  to write to :" + output_path)
    else:
        logger.info("input file not provided")
        output_path = "../output/report.csv"
        logger.info("will try  to write to :" + output_path)
    start = time.time()
    from jobs import recipe

    recipe.process(spark, input_file, output_path)
    end = time.time()
