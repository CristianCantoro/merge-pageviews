#!/usr/bin/env python3
"""
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

import os
import re
import csv
import gzip
import glob
import logging
import pathlib
import argparse
import tempfile
import datetime

import progressbar
progressbar.streams.wrap_stderr()


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


def concat_hours(hours_data):
    return 'foo'


date_pattern = r'[0-9]{8}-[0-9]{6}'
regex_date = re.compile(date_pattern)
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
                            help="Date to process (format: YYYYMMDD).")

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

    parser_list.add_argument('--output',
                             help="Name of output file "
                                  "[default: longest common substring "
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

        input_files = [f for f in glob.iglob(pathfile)]
        input_files_count = len(input_files)

        output_name = '{}.csv'.format(input_date_str)

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

        if args.output is None:
            basenames = [os.path.basename(inp) for inp in input_files]
            output_name = re.sub("[^\\w]$", "", long_substr(basenames))
        else:
            output_name = args.output


    logger.debug('output_name: {}'.format(output_name))

    input_files_count = len(input_files)
    logger.debug('input_files_count: {}'.format(input_files_count))
    if input_files_count < 1:
        logger.warn('No input files match: exiting')
        exit(1)

    # for input_file in input_files:
    #     print(input_file)
    # exit(0)

    output_path = os.path.join(outputdir, output_name)
    output_file = open(output_path, 'w+')
    output_writer = csv.writer(output_file, delimiter=' ')

    count_processed_input = 0
    count_total_lines = 0
    uncompressed_data = []
    with tempfile.NamedTemporaryFile(mode='w+', encoding=encoding) \
            as uncompressed_file:

        logger.debug('uncompressed_file: {}'.format(uncompressed_file.name))
        uncompressed_writer = csv.writer(uncompressed_file, delimiter=' ')

        with progressbar.ProgressBar(max_value=input_files_count) as barread:
            barread.update(0)
            for input_file in input_files:
                input_basename = os.path.basename(input_file)
                logger.debug('basename: {}'.format(input_basename))

                filename, file_ext = os.path.splitext(input_basename)

                timestamp = date_parser(
                                regex_date.search(input_basename).group()
                                )
                timestamp_str = timestamp.strftime("%Y%m%d-%H%M%S")
                logger.debug('timestamp: {}'.format(timestamp_str))

                with gzip.open(input_file,
                               "rt",
                               encoding='utf-8',
                               errors='replace') as infile:
                    compressed_reader = csv.reader(infile, delimiter=' ')

                    while True:
                        try:
                            line = next(compressed_reader)
                        except StopIteration:
                            break
                        except:
                            logger.warn('Error - Corrupted line: {}'
                                        .format(line))
                            continue

                        try:
                            lang = line[0]
                            page = line[1]
                            views = int(line[2])
                        except Exception as e:
                            logger.warn('Error - Corrupted line: {}'
                                    .format(e))
                            continue

                        count_total_lines += 1
                        uncompressed_data.append([lang,
                                                  page,
                                                  timestamp_str,
                                                  views
                                                  ])

                count_processed_input += 1
                barread.update(count_processed_input)

        logger.debug('count_total_lines: {}'.format(count_total_lines))
        logger.info('Writing data to uncompressed_file')
        count_written_lines = 0
        with progressbar.ProgressBar(max_value=count_total_lines) as barwrite:
            barwrite.update(0)

            for line in sorted(uncompressed_data):
                uncompressed_writer.writerow(line)
                uncompressed_file.flush()

                count_written_lines += 1
                barwrite.update(count_written_lines)
        logger.info('Data written to uncompressed_file')

        uncompressed_file.flush()
        uncompressed_file.seek(0)
        del uncompressed_data

        count_processed_lines = 0
        with progressbar.ProgressBar(max_value=count_total_lines) as barproc:
            barproc.update(0)

            old_lang = None
            old_page = None
            old_timestamp = None
            old_day = None
            daily_data = []
            hours_data = {}
            count_lines = 0

            uncompressed_reader = csv.reader(uncompressed_file,
                                             delimiter=' ')

            for line in uncompressed_reader:
                lang = line[0]
                page = line[1]

                timestamp = datetime.datetime.strptime(line[2],
                                                       '%Y%m%d-%H%M%S')
                day = timestamp.day

                views = int(line[3])

                if (old_lang is None or old_lang == lang) and \
                        (old_page is None or old_page == page) and \
                        (old_day is None or old_day == day):
                    # logger.debug('Save data.')
                    daily_data.append(views)
                    hours_data[timestamp] = views

                else:
                    # logger.debug('Change data.')
                    if daily_data and hours_data:

                        total_daily_views = sum(daily_data)
                        enc_hours_views = concat_hours(hours_data)

                        # import ipdb; ipdb.set_trace()
                        output_writer.writerow((old_lang,
                                                old_page,
                                                total_daily_views,
                                                enc_hours_views
                                                ))

                    daily_data = []
                    hours_data = {}

                    daily_data.append(views)
                    hours_data[timestamp] = views

                old_lang = lang
                old_page = page
                old_timestamp = timestamp
                old_day = day

                count_processed_lines += 1
                barproc.update(count_processed_lines)

logger.debug('All done!')
exit(0)
