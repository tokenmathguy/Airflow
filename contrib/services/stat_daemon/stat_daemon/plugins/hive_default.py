#!/usr/bin/env python
import logging
import time
import sys
from airflow.hooks import PrestoHook, HiveServer2Hook, HiveMetastoreHook


class HiveStats(object):

    """
    Compute hive stats

    This class does not need to be used, but can be helpful
    """

    def __init__(self, args):
        metastore = HiveMetastoreHook(args.metastore_conn_id)
        self.table = metastore.get_table(args.path.split('/')[0])
        self.partition_list = None
        if '/' in args.path:
            self.partition_list = args.path.split('/')[1:]
        self.presto_conn_id = None
        if 'presto_conn_id' in args:
            self.presto_conn_id = args.presto_conn_id
        self.hiveserver2_conn_id = None
        if 'hiveserver2_conn_id' in args:
            self.hiveserver2_conn_id = args.hiveserver2_conn_id
        self.path = args.path
        self.numeric_types = {'double', 'float', 'int', 'bigint', 'tinyint'}
        self.string_types = {'string', 'varchar'}
        self.null_stat = 'SUM(IF({col} IS NULL, 1, 0)) AS null_{col}'
        self.min_stat = 'MIN({col}) AS min__{col}'
        self.max_stat = 'MAX({col}) AS max__{col}'

    def use_presto(self):
        """
        Determines whether we can use presto to calculate stats
        """
        invalid_conn_id = {'none', 'n/a', ''}
        if self.presto_conn_id and \
                self.presto_conn_id.lower() not in invalid_conn_id:
            return True
        else:
            return False

    def default_numeric(self, col):
        """
        Column is numeric type
        """
        return ['SUM({col}) AS sum__{col}',
                'AVG({col}) AS avg__{col}',
                'STDDEV({col}) AS stddev__{col}',
                'VARIANCE({col}) AS variance__{col}',
                self.min_stat,
                self.max_stat]

    def default_boolean(self, col):
        """
        Column is boolean (may not be boolean type)
        """
        if col.type == 'boolean':
            return ['SUM(IF({col}, 1, 0)) AS true__{col}',
                    'SUM(IF(NOT {col}, 1, 0)) AS false__{col}']
        elif col.type in self.numeric_types:
            return ['SUM(IF({col}=0, 0, 1)) AS true__{col}',
                    'SUM(IF({col}=0, 1, 0)) AS false__{col}']
        elif col.type in self.string_types:
            return [("SUM(IF({col} IN ('T', '1', 'True', 'true', 'yes'), 1, 0))"
                     " AS true__{col}"),
                    ("SUM(IF({col} IN ('T', '1', 'True', 'true', 'yes'), 0, 1))"
                     " AS true__{col}")]
        else:
            return []

    def default_primary_key(self, col):
        """
        Column is a primary key
        """
        stats = self.default_key(col)
        if self.use_presto():
            stats += ['APPROX_DISTINCT({col}) AS approx_distinct__{col}']
        return stats

    def default_key(self, col):
        """
        Column is a foreign key
        """
        if col.type in self.numeric_types:
            return [self.max_stat, self.min_stat, self.null_stat]
        else:
            return [self.null_stat]

    def default_array(self, col):
        """
        Column is an array type
        """
        if col.type.startswith('array'):
            if self.use_presto():
                return [('AVG(IF(JSON_ARRAY_LENGTH({col}) >= 0, '
                         'JSON_ARRAY_LENGTH({col}), 0)) AS avg_size__{col}'),
                        ('MAX(IF(JSON_ARRAY_LENGTH({col}) >= 0, '
                         'JSON_ARRAY_LENGTH({col}), 0)) AS max_size__{col}'),
                        self.null_stat]
            else:
                return [('AVG(IF(SIZE({col}) >= 0, SIZE({col}), 0))'
                         ' AS avg_size__{col}'),
                        ('MAX(IF(SIZE({col}) >= 0, SIZE({col}), 0))'
                         ' AS max_size__{col}'),
                        self.null_stat]
        else:
            return []

    def default_string(self, col):
        """
        Column is a string
        """
        stats = []
        if self.use_presto():
            stats += ['APPROX_DISTINCT({col}) AS approx_distinct__{col}']
        stats += [('AVG(CAST(COALESCE(LENGTH({col}), 0) AS BIGINT))'
                   ' AS avg_len__{col}')]
        return stats

    def get_column_stats(self, col):
        """
        Returns a list of stats by column
        (Override this function to create custom mappings)
        """
        stats = ["COUNT({col}) AS non_null__{col}"]
        if col.type in self.numeric_types:
            stats += self.default_numeric(col)
        elif col.type == 'boolean':
            stats += self.default_boolean(col)
        elif col.type in self.string_types:
            stats += self.default_string(col)
        elif col.type.startswith('array'):
            stats += self.default_array(col)
        return [item.format(col=col.name) for item in stats]

    def get_stats(self):
        """
        Get a list of stats
        """
        stats = ["COUNT(1) AS count"]
        for col in self.table.sd.cols:
            stats += self.get_column_stats(col)
        return stats

    def get_hook(self):
        """
        Return either a hive of presto hook to compute the stats
        """
        if self.use_presto():
            return PrestoHook(presto_conn_id=self.presto_conn_id)
        else:
            return HiveServer2Hook(hiveserver2_conn_id=self.hiveserver2_conn_id)

    def get_data(self):
        """
        Generates the data
        """
        stats = self.get_stats()
        sql = "SELECT "
        sql += ", ".join(stats)
        sql += " FROM {db}.{table}".format(db=self.table.dbName,
                                           table=self.table.tableName)
        if self.partition_list:
            sql += " WHERE "
            sql += " AND ".join([p.replace('=', "='") + "'"
                                 for p in self.partition_list])
        hook = self.get_hook()
        rows = hook.get_records(hql=sql)
        if not rows:
            raise Exception("The query returned None")
        row = rows[0]
        if not row:
            raise Exception("The query returned None")
        path = 'hive:' + self.path
        return [[path, stat.lower().split('as ')[-1],
                 val, int(time.time())] for stat, val in zip(stats, row)]


def get_rows(args):
    """
    Default updater for hive
    The functions above can be re-used to implement a custom updater.
    """
    return HiveStats(args).get_data()
