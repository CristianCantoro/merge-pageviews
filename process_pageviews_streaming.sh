#!/usr/bin/env bash
# ---
#
# merge-pageviews: Merge Wikipedia's pagecounts-raw to get pagecounts-ez.
#
# Copyright (C) 2018 Critian Consonni for:
# * Eurecat - Centre Tecnològic de Catalunya
# * University of Trento, Department of Engineering and Computer Science (DISI)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# See the LICENSE file in this repository for further details.
# ---

# shellcheck disable=SC2128
SOURCED=false && [ "$0" = "$BASH_SOURCE" ] || SOURCED=true
if ! $SOURCED; then
  set -euo pipefail
  IFS=$'\n\t'
fi

# usage: merge_pageviews_sorted_by_time_streaming.py [-h]
#                                                    [--outputdir OUTPUTDIR]
#                                                    [--encoding ENCODING]
#                                                    {day,list} ...
#
# Merge Wikipedia's pagecounts-raw to get pagecounts-ez.
#
# optional arguments:
#   -h, --help            show this help message and exit
#   --outputdir OUTPUTDIR
#                         Where the directory with the elaborated data will be
#                         saved [default: '.'].
#   --encoding ENCODING   Encoding of input files [default: 'utf-8'].
#
# subcommands:
#   valid subcommands
#
#   {day,list}            additional help
#
# usage: merge_pageviews_sorted_by_time_streaming.py day [-h]
#                                                        [--datadir DATADIR]
#                                                        [--basename BASENAME]
#                                                        [--extension EXTENSION]
#                                                        <date>
#
# positional arguments:
#   <date>                Date to process (avalilable formats: YYYYMMDD or YYYY-
#                         MM-DD).
#
# optional arguments:
#   -h, --help            show this help message and exit
#   --datadir DATADIR     Path where the pagecount files are located [default:
#                         '.'].
#   --basename BASENAME   Path where the pagecount files are located [default:
#                         'pagecounts-'].
#   --extension EXTENSION
#                         Extension of the pagecount files[default: '.gz'].
function usage() {
  (>&2 echo \
"Usage:
  process_pageviews_streaming.sh [options]

Sync date on the machine with time from google.com.

Options:
   --outputdir OUTPUTDIR
                         Where the directory with the elaborated data will be
                         saved [default: '.'].
   --encoding ENCODING   Encoding of input files [default: 'utf-8'].

   --datadir DATADIR     Path where the pagecount files are located [default:
                         '.'].
   --basename BASENAME   Path where the pagecount files are located [default:
                         'pagecounts-'].
   --extension EXTENSION
                         Extension of the pagecount files[default: '.gz'].

  -h, --help             Show this help and exits.

Example:
  ssh-fingerprint localhost")
}

function cli_args() {
  while getopts ":oebdxh" opt; do
    case $opt in
      o)
        (>&2 echo "-o was triggered, Parameter: $OPTARG")
        ;;
      e)
        (>&2 echo "-e was triggered, Parameter: $OPTARG")
        ;;
      b)
        (>&2 echo "-b was triggered, Parameter: $OPTARG")
        ;;
      d)
        (>&2 echo "-d was triggered, Parameter: $OPTARG")
        ;;
      x)
        (>&2 echo "-x was triggered, Parameter: $OPTARG")
        ;;
      h)
        usage
        exit 0
        ;;
      \?)
        (>&2 echo "Invalid option: -$OPTARG")
        exit 1
        ;;
      :)
        (>&2 echo "Option -$OPTARG requires an argument.")
        exit 1
        ;;
      *)
        (>&2 echo "Flag $opt not recognized." )
        exit 1
        ;;
    esac
  done
}


function main() {

  cli_args "$@"

}


main "$@"