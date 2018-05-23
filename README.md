merge-pageviews
---------------

This scripts can be used to merge Wikipedia's
[pagecounts-raw](https://wikitech.wikimedia.org/wiki/Analytics/Archive/Data/Pagecounts-raw)
to get [pagecounts-ez](https://dumps.wikimedia.org/other/pagecounts-ez/).

## Usage

```
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
./import_pageviews_sorted_by_time_spark.py \
    --datadir data/input/sorted_time/2007-12/ \
    --outputdir output \
    20071210
```

## License

This project is realease unde GPL v3 (or later).

```
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
```

See the LICENSE file in this repository for further details.
