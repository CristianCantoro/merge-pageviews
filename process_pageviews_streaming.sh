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

tmpdir=$(mktemp -d -t tmp.merge-pageviews.XXXXXXXXXX)
function finish {
  rm -rf "$tmpdir"
}
trap finish EXIT


#################### Utils
echodebug() {


  (>&2 echo -en "[$(date '+%F_%k:%M:%S')][DEBUG]\\t")
  (>&2 echo "$@")
}
####################

function short_usage() {
  (>&2 echo "\
Usage: process_pageviews_streaming.sh [-o OUTPUTDIR] [-e ENCODING]
                                      [-d DATADIR] [-b BASENAME]
                                      [-x EXTENSION]
                                      <start_date> <end_date>

See process_pageviews_streaming.sh -h for further information.")
}

function usage() {
  (>&2 echo "\
Usage:
  process_pageviews_streaming.sh [-o OUTPUTDIR] [-e ENCODING] [-d DATADIR]
                                 [-b BASENAME] [-x EXTENSION]
                                 <start_date> <end_date>

Process pageviews from <start_date> to <end_date>.

Options:
  -o OUTPUTDIR     Where the directory with the elaborated data will be
                   saved [default: '.'].
  -e ENCODING      Encoding of input files [default: 'utf-8'].
  -n               Do not compress the output (default: compress wuth bz2).
  -j NJOBS         Number of parallel jobs [default: 1].
  -d DATADIR       Path where the pagecount files are located [default: '.'].
  -b BASENAME      Basename of pagecount files [default: 'pagecounts-'].
  -x EXTENSION     Extension of the pagecount files[default: '.gz'].
  -t DELAY         Delay between parallel jobs [default: 0].
  -r RESULTS       Directory where to save parallel output
                   [default: 'pageviews-results'].
  -h, --help       Show this help and exits.

Example:
  process_pageviews_streaming.sh 20071210")
}

outputdir='.'
datadir='.'
encoding='utf-8'
compress=true
input_basename='pagecounts-'
extension='.gz'
njobs=1
delay=''
results='pageviews-results'

function cli_args() {
  while getopts ":o:e:nj:b:d:x:t:r:h" opt; do
    case $opt in
      o)
        outputdir="$OPTARG"
        ;;
      e)
        encoding="$OPTARG"
        ;;
      n)
        compress=false
        ;;
      j)
        njobs="$OPTARG"
        ;;
      d)
        datadir="$OPTARG"
        ;;
      b)
        input_basename="$OPTARG"
        ;;
      x)
        extension="$OPTARG"
        ;;
      t)
        delay="$OPTARG"
        ;;
      r)
        results="$OPTARG"
        ;;
      h)
        usage
        exit 0
        ;;
      \?)
        (>&2 echo "Error. Invalid option: -$OPTARG")
        short_usage
        exit 1
        ;;
      :)
        (>&2 echo "Error. Option -$OPTARG requires an argument.")
        short_usage
        exit 1
        ;;
    esac
  done

  local args
  args=( "$@" )
  ARG1="${args[$(( ${OPTIND:-1} - 1))]}"
  ARG2="${args[$(( ${OPTIND:-1} ))]}"

  echodebug '$#:' "$#"
  echodebug "OPTIND: ${OPTIND:-1}"

  local numargs
  numargs=$(( $# - ${OPTIND:-1} + 1 ))

  local start start_seconds
  local end end_seconds
  if ((numargs == 2)); then
    start="${ARG1}"
    end="${ARG2}"

    start_seconds=$(date +"%s" -d "$start")
    end_seconds=$(date +"%s" -d "$end")

    if [ "$end_seconds" -lt "$start_seconds" ]; then
        (>&2 echo "Error. <end_date> must be greater or equal to"\
                  "<start_date>.")
        exit 2
    fi

    start_date=$(date +"%Y-%m-%d" -d "$start")
    end_date=$(date +"%Y-%m-%d" -d "$end")
  else
    (>&2 echo "Error. Two positional arguments required <start_date> and"\
              "<end_date>.")
    exit 1
  fi
}


function main() {

  cli_args "$@"

  echodebug "outputdir: $outputdir"
  echodebug "encoding: $encoding"
  echodebug "compress: $compress"
  echodebug "njobs: $njobs"
  echodebug "datadir: $datadir"
  echodebug "input_basename: $input_basename"
  echodebug "extension: $extension"
  echodebug "delay: $delay"

  echodebug "start_date: $start_date"
  echodebug "end_date: $end_date"

  now_seconds=$(date +"%s" -d "$start_date")
  end_seconds=$(date +"%s" -d "$end_date")

  date_list="${tmpdir}/dates_to_process.txt"
  touch "$date_list"
  while [ "$now_seconds"  -le "$end_seconds" ]; do
    now=$(date +"%Y-%m-%d" -d "@${now_seconds}")
    now_month=$(date +"%Y-%m" -d "@${now_seconds}")
    # echodebug "--> now: $now"
    echo "$now $now_month" >> "$date_list"

    now=$(date +"%Y-%m-%d" -d "$now + 1 day");
    now_seconds=$(date +"%s" -d "$now");
  done

  declare -a parallel_options
  parallel_options=( )
  if [ ! -z "$delay" ]; then
    parallel_options+=('--delay' "$delay")
  fi

  declare -a merge_options
  merge_options=( )
  if ! $compress; then
    merge_options+=( '--no-compress' )
  fi

  set -x
  # ./merge_pageviews_sorted_by_time_streaming.py
  #     --outputdir data/output/streaming/
  #         day --datadir ./data/input/sorted_time/2007-12/
  #           20071211

  parallel "${parallel_options[@]:+}" \
           --results "$results" \
           --jobs "$njobs" \
           --colsep ' ' \
           --progress \
      ./merge_pageviews_sorted_by_time_streaming.py \
        "${merge_options[@]:+}" \
        --outputdir "$outputdir" \
        --encoding "$encoding" \
            day --basename "$input_basename" \
                --extension "$extension" \
                --datadir "${datadir}/{2}" \
                  "{1}" :::: "$date_list"

}


main "$@"