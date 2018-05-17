#!/usr/bin/env python3

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

import pandas as pd
import argparse
import datetime
import tempfile
import gzip
import csv
import os
import logging
import progressbar

progressbar.streams.wrap_stderr()

import findspark
findspark.init()

import pyspark
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, TimestampType

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


def date_parser(timestamp):
    return datetime.datetime.strptime(timestamp, '%Y%m%d-%H%M%S')


def cli_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("FILE",
                        help="Input file.")
    args = parser.parse_args()

    parser.add_argument("--encoding",
                        help="Encoding of input files.",
                        default='utf-8',
                        nargs='+')
    args = parser.parse_args()

    return args

# The same thing more concise, and perhaps faster as it doesn't use a list: 
# df = pd.concat((pd.read_csv(f) for f in all_files)) 

if __name__ == "__main__":
    args = cli_args()

    sc = pyspark.SparkContext(appName="merge-pagecounts")
    sqlctx = pyspark.SQLContext(sc)

    schema = StructType([StructField("lang", StringType(), False),
                         StructField("page", StringType(), False),
                         StructField("views", IntegerType(), False),
                         StructField("timestamp", TimestampType(), True)])

    input_file = args.FILE
    encoding = args.encoding

    df = sqlctx.read.csv(input_file,
                         header=True,
                         mode="DROPMALFORMED",
                         schema=schema)
