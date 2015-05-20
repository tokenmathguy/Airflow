#!/usr/bin/env python
import logging
import time
import sys
from airflow.hooks import PrestoHook, HiveMetastoreHook


class HiveStats(object):

    def __init__(self, args):
        metastore = HiveMetastoreHook(args.metastore_conn_id)
        self.table = metastore.get_table(args.path.split('/')[0])
        self.partition_list = args.path.split('/')[1:]
        self.presto_conn_id = args.presto_conn_id
        self.path = args.path

    def get_column_stats(self, col):
        """
        Returns a list of stats by column
        (Override this function to create custom mappings)
        """
        stats = ["COUNT({col}) AS non_null__{col}"]
        if col.type in {'double', 'int', 'bigint', 'float'}:
            stats += ['SUM({col}) AS sum__{col}',
                      'MIN({col}) AS min__{col}',
                      'MAX({col}) AS max__{col}',
                      'AVG({col}) AS avg__{col}']
        elif col.type == 'boolean':
            stats += ['SUM(CASE WHEN {col} THEN 1 ELSE 0 END) AS true__{col}',
                      ('SUM(CASE WHEN NOT {col} THEN 1 ELSE 0 END) '
                       'AS false__{col}')]
        elif col.type == 'string':
            stats += ['SUM(CAST(LENGTH({col}) AS BIGINT)) AS len__{col}',
                      'APPROX_DISTINCT({col}) AS approx_distinct__{col}']
        return [item.format(col=col.name) for item in stats]

    def get_stats(self):
        """
        Get a list of stats
        """
        stats = ["COUNT(1) AS count"]
        for col in self.table.sd.cols:
            stats += self.get_column_stats(col)
        return stats

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
        hook = PrestoHook(presto_conn_id=self.presto_conn_id)
        row = hook.get_first(hql=sql)
        if not row:
            raise Exception("The query returned None")
        return [['hive', self.path, stat.lower().split('as ')[-1],
                 val, int(time.time())] for stat, val in zip(stats, row)]


def get_rows(args):
    """
    Default updater for hive
    The functions above can be re-used to implement a custom updater.
    """
    return HiveStats(args).get_data()
