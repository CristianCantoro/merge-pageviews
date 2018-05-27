Merge Wikipedia pageviews with Python 
-------------------------------------

## process

TODO

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
