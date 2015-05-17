# Stat Daemon
----

`stat_daemon` is a utility passively monitors for changes to metadata and updates it accordingly.  Current supported data types are hive tables, hdfs directories and s3 buckets.  Data is pushed to a MySQL (or Sqlite) table with the following schema:
```SQL
CREATE TABLE {table_name} (
    type    CHAR(32) NOT NULL,
    path    TEXT NOT NULL,
    stat    CHAR(64),
    val     FLOAT,
    ts      INT
);
```
The columns are defined as follows:
* type: type of data (hive, hdfs, etc.)
* path: hive table name (with db), hdfs path, etc.
* stat: type of stat (sum, count, size, etc.)
* val: value of the stat
* ts: creation timestamp

Type and path together are required to form a unique key.  (In rare cases, path might collide for different types).

## Quickstart
```bash
# instantiate the database:
stat_daemon create_table

# start the daemon
stat_daemon start --taskfolder /path/to/tasks
```

## Example Job Definition File
Stats job files are .py files that live in /path/to/tasks (similar to Airflow DAG definition files).
```py
from airflow.hooks import HiveMetastoreHook
from stat_daemon.task import StatsQueue, HiveStats, HdfsStats

queue = StatsQueue() #contains stats that we will track
metastore_hook = HiveMetastoreHook()
db = 'default'
# add all tables in default namespace
for table in metastore_hook.get_tables(db):
    path = db + '.' + table.tableName
    # if there are partitions, process each sub-partition as a separate path
    if table.partitionKeys:
        path += '/'
        path += '/'.join(['{}=%'.format(p.name) for p in table.partitionKeys])
    HiveStats(
            path=path,
            queue=queue
    )
```

## Configuring
The CLI provides more fine-grained control over how jobs are run:
```bash
# Update max 1000 items per cycle, sleep 1 hour before updating
stat_daemon start --taskfolder /path/to/tasks --maxjobs=1000 --sleep=3600
```

## Plugins
The stats collected are completely modular.  Simply add a reference to your
plugin in the stats definition file:
```py
HiveStats(
        path=path,
        plugin='/path/to/my/hive_plugin.py',
        queue=queue
)
```
The plugin defines a function that returns one or more rows of data:
```py
# simple example plugin
#!/usr/bin/env python
import logging
import time

def get_rows(args):
    """
    Demo updater
    """
    try:
        row1 = ['demo', args.path,  'stat1', 1, int(time.time())]
        row2 = ['demo', args.path,  'stat2', 2, int(time.time())]
        return [row1, row2]
    except:
        logging.error("Failed to process: {}".format(args.path))
```


## Other options
The command-line utility provides some helper functions for communicating with the database:
```bash
# instantiate the database (add --drop flag to re-create existing db):
stat_daemon create_table
# show records from the database:
stat_daemon show_stats --limit 5
"   type                                  path      stat  val          ts
0  hive  default.test_table/ds=2008-05-28  non_null    1  1431651421
1  hive  default.test_table/ds=2008-05-31  non_null    1  1431651422
2  hive  default.test_table/ds=2008-06-18  non_null    1  1431651423
3  hive  default.test_table/ds=2008-06-24  non_null    1  1431651423
4  hive  default.test_table/ds=2008-07-08  non_null    1  1431651424"
# clear records from the database:
stat_daemon clear_stats --path default.test_table/ds=2008% --type hive
```
