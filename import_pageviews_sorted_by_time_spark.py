#!/usr/bin/env python3

# CREATE TABLE pageviews (
#     lang varchar(8),
#     request test,
#     timestamp timestamp,
#     views integer CHECK (views>0),
#     reqbytes integer CHECK (reqbytes>0)
# );

# How to copy from CSV file to PostgreSQL table with headers in CSV file?
# https://stackoverflow.com/q/17662631/2377454

initial_comment=\
"""# Wikimedia page request counts for 16/11/2011 (dd/mm/yyyy)
#
# Each line shows 'project page daily-total hourly-counts'
#
# Project is 'language-code project-code'
#
# Project-code is
#
# b:wikibooks,
# k:wiktionary,
# n:wikinews,
# q:wikiquote,
# s:wikisource,
# v:wikiversity,
# wo:wikivoyage,
# z:wikipedia (z added by merge script: wikipedia happens to be sorted last in dammit.lt files, but without suffix)
#
# Counts format: only hours with page view count > 0 (or data missing) are represented,
#
# Hour 0..23 shown as A..X (saves up to 22 bytes per line compared to comma separated values), followed by view count.
# If data are missing for some hour (file missing or corrupt) a question mark (?) is shown,
# and a adjusted daily total is extrapolated as follows: for each missing hour the total is incremented with hourly average
#
# Page titles are shown unmodified (preserves sort sequence)
#"""

import os
import csv
import gzip
import glob
import pathlib
import argparse
import datetime
import logging
import progressbar

progressbar.streams.wrap_stderr()

import findspark
findspark.init()

import pyspark
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, TimestampType
from pyspark.sql import functions
from pyspark.sql.functions import lit

########## logging
# create logger with 'spam_application'
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter and add it to the handlers
formatter = logging.Formatter('[%(asctime)s][%(levelname)s]: %(message)s')
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(ch)
##########


def unionAll(*dfs):
    first, *_ = dfs  # Python 3.x, for 2.x you'll have to unpack manually
    return first.sql_ctx.createDataFrame(
        first.sql_ctx._sc.union([df.rdd for df in dfs]),
        first.schema
    )


def date_parser(timestamp):
    return datetime.datetime.strptime(timestamp, '%Y%m%d-%H%M%S')


def cli_args():


    def valid_date_type(arg_date_str):
        """custom argparse *date* type for user dates values given from the
           command line"""
        try:
            return datetime.datetime.strptime(arg_date_str, "%Y%m%d")
        except ValueError:
            msg = "Given Date ({0}) not valid! Expected format, YYYYMMDD!"\
                  .format(arg_date_str)
            raise argparse.ArgumentTypeError(msg)

    parser = argparse.ArgumentParser()

    parser.add_argument("date",
                        metavar='<date>',
                        type=valid_date_type,
                        help="Date to process.")

    parser.add_argument("--datadir",
                        type=pathlib.Path,
                        default=os.getcwd(),
                        help="ath where the pagecount files are located "
                             "[default: '.'].")

    parser.add_argument("--basename",
                        default='pagecounts-',
                        help="Path where the pagecount files are located "
                             "[default: 'pagecounts-'].")

    parser.add_argument("--outputdir",
                        type=pathlib.Path,
                        default=os.getcwd(),
                        help="Where the directory with the elaborated data "
                             "will be saved [default: '.'].")

    parser.add_argument("--extension",
                        default='.gz',
                        help="Extension of the pagecount files"
                             "[default: '.gz'].")

    parser.add_argument("--encoding",
                        default='utf-8',
                        help="Encoding of input files [default: utf-8].")

    args = parser.parse_args()

    return args

if __name__ == "__main__":
    args = cli_args()

    sc = pyspark.SparkContext(appName="merge-pagecounts")
    sqlctx = pyspark.SQLContext(sc)

    schema = StructType([StructField("lang", StringType(), False),
                         StructField("page", StringType(), False),
                         StructField("views", IntegerType(), False),
                         StructField("reqbytes", IntegerType(), False)])

    input_date = args.date
    input_date_str = input_date.date().strftime('%Y%m%d')

    datadir = os.path.abspath(args.datadir)
    outputdir = os.path.abspath(args.outputdir)
    basename = args.basename
    encoding = args.encoding
    extension = args.extension

    fileglob = basename + input_date_str + '*' + extension
    pathfile = os.path.join(datadir, fileglob)
    logger.debug('pathfile: {}'.format(pathfile))

    list_dfs = list()
    for input_file in glob.iglob(pathfile):
        logger.debug('input_file: {}'.format(input_file))

        timestamp = date_parser(os.path.basename(input_file)
                                       .replace('pagecounts-','')
                                       .replace('.gz',''))

        logger.info('Processing file: {}'.format(input_file))
        tmp_spark_df = sqlctx.read.csv(
                            input_file,
                            header=False,
                            schema=schema,
                            sep=' ')

        tmp_spark_df = tmp_spark_df.withColumn("timestamp", lit(timestamp))
        list_dfs.append(tmp_spark_df)
        del tmp_spark_df

        logger.info('Added DataFrame for file {} to list'.format(input_file))

    logger.info('Union of all Spark DataFrames.')
    df = unionAll(*list_dfs)

    logger.info('Spark DataFrame created')

    df = df.drop('reqbytes')

    grouped_df = (df
        .select(['lang',
                 'page',
                 functions.date_format('timestamp','yyyy-MM-dd')\
                          .alias('day'),
                 'views'])
        .groupby(['lang','page','day'])
        .sum('views')
        )

    grouped_df.write.csv(os.path.join(outputdir, input_date_str),
                         header=True,
                         sep='\t')

