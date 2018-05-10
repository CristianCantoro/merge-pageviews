#!/usr/bin/env python3

import pandas as pd
import argparse
import datetime
import tempfile
import gzip
import csv
import os

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


def date_parser(timestamp):
    return datetime.datetime.strptime(timestamp, '%Y%m%d-%H%M%S')


def cli_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("FILE",
                        help="Input file.",
                        nargs='+')
    args = parser.parse_args()

    parser.add_argument("--encoding",
                        help="Encoding of input files.",
                        default='utf-8',
                        nargs='+')
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = cli_args()

    input_files = args.FILE
    encoding = args.encoding

    list_dfs = list()
    for input_file in input_files:

        timestamp = date_parser(os.path.basename(input_file)
                                       .replace('pagecounts-','')
                                       .replace('.gz',''))

        with tempfile.NamedTemporaryFile(mode='w+', encoding=encoding) \
                as uncompressed_file:
            
            writer = csv.writer(uncompressed_file, delimiter='\t', quoting=csv.QUOTE_ALL)
            with gzip.open(input_file, "rt", encoding='utf-8', errors='replace') as infile:
                reader = csv.reader(infile, delimiter=' ')


                while True:

                    try:
                        line = next(reader)
                    except StopIteration:
                        break
                    except:
                        continue

                    try:
                        lang = line[0]
                        page = line[1]
                        views = int(line[2])
                        reqbytes = int(line[3])
                    except:
                        pass

                    writer.writerow((lang, page, views, reqbytes))

                uncompressed_file.seek(0)

                # import ipdb; ipdb.set_trace()
                tmp_df = pd.read_csv(uncompressed_file,
                                     sep='\t',
                                     names=['lang', 'page', 'views',
                                            'reqbytes'],
                                     dtype={'lang': str,
                                            'page': str,
                                            'views': int,
                                            'reqbytes': int
                                            },
                                     header=None,
                                     encoding='utf-8',
                                     error_bad_lines=False,
                                     warn_bad_lines=True,
                                     low_memory=True
                                     )

                tmp_df['timestamp'] = timestamp
                list_dfs.append(tmp_df)


    df = pd.concat(list_dfs)

    del tmp_df
    del list_dfs
