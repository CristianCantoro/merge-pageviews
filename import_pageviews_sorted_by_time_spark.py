#!/usr/bin/env python3
"""
usage: import_pageviews_sorted_by_time_spark.py [-h] [--outputdir OUTPUTDIR]
                                                [--encoding ENCODING]
                                                {day,list} ...

Merge Wikipedia's pagecounts-raw to get pagecounts-ez.

optional arguments:
  -h, --help            show this help message and exit
  --outputdir OUTPUTDIR
                        Where the directory with the elaborated data will be
                        saved [default: '.'].
  --encoding ENCODING   Encoding of input files [default: 'utf-8'].

subcommands:
  valid subcommands

  {day,list}            additional help
---

merge-pageviews: Merge Wikipedia's pagecounts-raw to get pagecounts-ez.

Copyright (C) 2018 Critian Consonni for:
* Eurecat - Centre Tecnològic de Catalunya
* University of Trento, Department of Engineering and Computer Science (DISI)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

See the LICENSE file in this repository for further details.
"""

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
import pandas as pd

import progressbar
progressbar.streams.wrap_stderr()

import findspark
findspark.init()

import pyspark
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, TimestampType
from pyspark.sql import functions
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import lit
from pyspark.sql.functions import col

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

# taken from
# Spark union of multiple RDDs
# https://stackoverflow.com/a/33744540/2377454
def unionAll(*dfs):
    first, *_ = dfs  # Python 3.x, for 2.x you'll have to unpack manually
    return first.sql_ctx.createDataFrame(
        first.sql_ctx._sc.union([df.rdd for df in dfs]),
        first.schema
    )


def date_parser(timestamp):
    return datetime.datetime.strptime(timestamp, '%Y%m%d-%H%M%S')


new_schema = StructType([StructField("lang", StringType(), False),
                         StructField("page", StringType(), False),
                         StructField("day", StringType(), False),
                         StructField("enc", StringType(), False)])


hour_to_letter = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O',
              'P','Q','R','S','T','U','V','W','X']


@pandas_udf(new_schema, PandasUDFType.GROUPED_MAP)
def concat_hours(x):
    view_hours = x['hour'].tolist()
    view_views = x['views'].tolist()

    view_hours_letters = [hour_to_letter[h] for h in view_hours]

    encoded_views = [l + str(h) for l, h
                     in sorted(zip(view_hours_letters,view_views))]
    encoded_views_string = ''.join(encoded_views)


    # See this bug report:
    # "UserDefinedFunction mixes column labels"
    # https://issues.apache.org/jira/browse/SPARK-24324
    #
    # return pd.DataFrame({'lang': x.lang,
    #                      'page': x.page,
    #                      'day': x.day,
    #                      'enc': encoded_views_string
    #                      }, index=[x.index[0]])
    return pd.DataFrame({'enc': x.page,
                         'day': x.lang,
                         'lang': x.day,
                         'page': encoded_views_string
                         }, index=[x.index[0]])


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


    parser = argparse.ArgumentParser(
        description="Merge Wikipedia's pagecounts-raw to get pagecounts-ez.")

    subparsers = parser.add_subparsers(title='subcommands',
                                       description='valid subcommands',
                                       help='additional help')
    subparsers.required = True
    subparsers.dest = 'subcommand'

    # create the parser for the "foo" command
    parser_day = subparsers.add_parser('day')

    parser_day.add_argument("date",
                            metavar='<date>',
                            type=valid_date_type,
                            help="Date to process.")

    parser_day.add_argument("--datadir",
                            type=pathlib.Path,
                            default=os.getcwd(),
                            help="Path where the pagecount files are located "
                                 "[default: '.'].")

    parser_day.add_argument("--basename",
                            default='pagecounts-',
                            help="Path where the pagecount files are located "
                                 "[default: 'pagecounts-'].")

    parser_day.add_argument("--extension",
                            default='.gz',
                            help="Extension of the pagecount files"
                                 "[default: '.gz'].")

    # create the parser for the "bar" command
    parser_list = subparsers.add_parser('list')
    parser_list.add_argument('input_files',
                             metavar='<file>',
                             nargs='+',
                             help="List of files to process.")

    parser_list.add_argument('--resultdir',
                             help="Name of the directory containing the "
                                  "results [default: longest common substring "
                                  "of the input file].")

    parser.add_argument("--outputdir",
                        type=pathlib.Path,
                        default=os.getcwd(),
                        help="Where the directory with the elaborated data "
                             "will be saved [default: '.'].")


    parser.add_argument("--encoding",
                        default='utf-8',
                        help="Encoding of input files [default: 'utf-8'].")

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

    outputdir = os.path.abspath(str(args.outputdir))
    encoding = args.encoding

    if args.subcommand == 'day':
        input_date = args.date
        input_date_str = input_date.date().strftime('%Y%m%d')

        datadir = os.path.abspath(str(args.datadir))
        basename = args.basename
        extension = args.extension

        fileglob = basename + input_date_str + '*' + extension
        pathfile = os.path.join(datadir, fileglob)
        logger.debug('pathfile: {}'.format(pathfile))

        input_files = len([f for f in glob.iglob(pathfile)])

        result_dirname = input_date_str

    else:

      import re
      def long_substr(data):
            substr = ''
            if len(data) > 1 and len(data[0]) > 0:
                for i in range(len(data[0])):
                    for j in range(len(data[0])-i+1):
                        if j > len(substr) and \
                                all(data[0][i:i+j] in x for x in data):
                            substr = data[0][i:i+j]
            return substr


      input_files = args.input_files

      basenames = [os.path.basename(inp) for inp in input_files]
      result_dirname = re.sub("[^\\w]$", "", long_substr(basenames))

    logger.debug('result_dirname: {}'.format(result_dirname))

    input_files_count = len(input_files)
    logger.debug('input_files_count: {}'.format(input_files_count))
    if input_files_count < 1:
        logger.warn('No input files match: exiting')
        exit(1)

    list_dfs = list()
    with progressbar.ProgressBar(max_value=input_files_count) as bar:
        for input_file in input_files:
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
            bar.update(len(list_dfs))

    assert len(list_dfs) >= 1, 'There should be at least one DataFrame'

    if len(list_dfs) > 1:
        logger.info('Union of all Spark DataFrames.')
        df = unionAll(*list_dfs)
        logger.info('Spark DataFrame created')
    else:
        df = list_dfs[0]

    logger.info('Dropping column "reqbytes" from DataFrame')
    df = df.drop('reqbytes')
    logger.info('Dropped column "reqbytes" from DataFrame')

    logger.info('Aggregating total views by day.')
    grouped_daily_df = (df.select(['lang',
                                   'page',
                                   functions.date_format('timestamp','yyyy-MM-dd')\
                                            .alias('day'),
                                   'views'])
                          .groupby(['lang','page','day'])
                          .sum('views')
                          .dropDuplicates()
                          )
    grouped_daily_df = grouped_daily_df.select([col('lang').alias('lang_d'),
                                            col('page').alias('page_d'),
                                            col('day').alias('day_d'),
                                            col('sum(views)').alias('daily_views'),
                                            ])
    logger.info('Aggregated total views by day.')

    logger.info('Concatenating hourly views.')
    grouped_hours_df = (df.select(['lang',
                                   'page',
                                   functions.date_format('timestamp','yyyy-MM-dd')
                                            .alias('day'),
                                   functions.hour('timestamp').alias('hour'),
                                   'views'
                                   ])
                          .groupby(['lang','page','day'])
                          .apply(concat_hours)
                          .dropDuplicates()
                          )
    logger.info('Concatenated hourly views.')

    cond = [grouped_daily_df.lang_d == grouped_hours_df.lang,
            grouped_daily_df.page_d == grouped_hours_df.page,
            grouped_daily_df.day_d == grouped_hours_df.day]
    final = (grouped_daily_df.join(grouped_hours_df, cond)
                             .select('lang_d', 'page_d', 'day_d', 'daily_views', 'enc')
                             .dropDuplicates()
                             )

    logger.info('Writing results to disk ...')
    final.write.csv(os.path.join(outputdir, result_dirname),
                         header=True,
                         sep='\t')

    logger.info('All done!')
