Merge Wikipedia pageviews with Python 
-------------------------------------

## process

This script launches the merge script
`merge_pageviews_sorted_by_time_streaming.py` (with `day` subcommand),
over a range of dates.

```
 ./process_pageviews_streaming.sh -h
Usage:
  process_pageviews_streaming.sh [-o OUTPUTDIR] [-e ENCODING] [-n] [-j NJOBS]
                                 [-d DATADIR] [-b BASENAME] [-x EXTENSION]
                                 [-t DELAY] [-r RESULTS]
                                 <start_date> <end_date>
  process_pageviews_streaming.sh -h

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
  process_pageviews_streaming.sh 20071210 20071211
```

#### Example
```
./process_pageviews_streaming.sh \
        -o ./data/output \
        -d ./data/input \
        -r ./process-results/process_20080101-20111115 \
        -j 5 \
        -t 5m \
        20080101 20111115


## merge
```
usage: merge_pageviews_sorted_by_time_streaming.py [-h]
                                                   [--outputdir OUTPUTDIR]
                                                   [--encoding ENCODING]
                                                   [--no-compress]
                                                   {day,list} ...

Merge Wikipedia's pagecounts-raw to get pagecounts-ez.

optional arguments:
  -h, --help            show this help message and exit
  --outputdir OUTPUTDIR
                        Where the directory with the elaborated data will be
                        saved [default: '.'].
  --encoding ENCODING   Encoding of input files [default: 'utf-8'].
  --no-compress         Do not compress the output (default compresses with
                        bz2).

subcommands:
  valid subcommands

  {day,list}            additional help
```

### `day` subcommand

```
usage: import_pageviews_sorted_by_time_spark.py day [-h] [--datadir DATADIR]
                                                    [--basename BASENAME]
                                                    [--extension EXTENSION]
                                                    <date>

positional arguments:
  <date>                Date to process.

optional arguments:
  -h, --help            show this help message and exit
  --datadir DATADIR     Path where the pagecount files are located [default:
                        '.'].
  --basename BASENAME   Path where the pagecount files are located [default:
                        'pagecounts-'].
  --extension EXTENSION
                        Extension of the pagecount files[default: '.gz'].
```

### `list` subcommand

```
usage: import_pageviews_sorted_by_time_spark.py list [-h]
                                                     [--resultdir RESULTDIR]
                                                     <file> [<file> ...]

positional arguments:
  <file>                List of files to process.

optional arguments:
  -h, --help            show this help message and exit
  --resultdir RESULTDIR
                        Name of the directory containing the results [default:
                        longest common substring of the input file].
```

#### Example

To merge the data for a single day, assuming that:
* you have downloaded the pagecounts files `pagecounts-20071210-*.gz` to your
  local folder `data/input/sorted_time/2007-12/`;
* you want to save the output in a directory called `output`;
* you want to merge the pageviews for `2007-12-10`;

you can launch the script like this:
```bash
./import_pageviews_sorted_by_time_streaming.py \
  --outputdir data/output/streaming/ \
    day --datadir ./data/input/sorted_time/2007-12/ 20071212
```
