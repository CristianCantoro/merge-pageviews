merge-pageviews
---------------

These scripts can be used to merge Wikipedia's
[pagecounts-raw](https://wikitech.wikimedia.org/wiki/Analytics/Archive/Data/Pagecounts-raw)
to get [pagecounts-ez](https://dumps.wikimedia.org/other/pagecounts-ez/).

There are two sets of scripts:
* Scripts using Apache Spark: `process_pageviews_streaming.sh`, and
  `merge_pageviews_sorted_by_time_streamin.py` , see [docs/streaming.md](./docs/streaming.md) 
* Scripts using Apache Spark: `process_pageviews_spark.sh` , and
  `merge_pageviews_sorted_by_time_spark.py` see [docs/spark.md](./docs/spark.md)

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
