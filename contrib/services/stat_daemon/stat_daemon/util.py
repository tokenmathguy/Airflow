import copy
import datetime
import imp
import json
import glob
import logging
import os
import sys

from airflow.hooks import HDFSHook, MySqlHook, PrestoHook, SqliteHook

def _get_sql_hook(sql_conn_id):
    """
    Local helper function to get a SQL hook
    """
    if 'sqlite' in sql_conn_id:
        return SqliteHook(sql_conn_id)
    else:
        return MySqlHook(sql_conn_id)


def require_args(args, req):
    """
    Custom require args (args might be modified by code)
    """
    error = None
    msg = "Argument --{} not specified"
    for k, v in vars(args).iteritems():
        if k in req and not v:
            logging.error(msg.format(k))
            error = True
    for item in req:
        if not item in vars(args):
            logging.error(msg.format(item))
            error = True
    if error:
        sys.exit(1)


def update_args(args, task):
    """
    Override arguments
    """
    args_ = copy.deepcopy(args)
    args_.type = task.type
    args_.path = task.path
    if task.plugin:
        args_.plugin = task.plugin
    if task.plugin_args:
        args_.plugin_args = "'{}'".format(json.dumps(task.plugin_args))
    return args_


class MetadataTable:

    """
    Interface to the metadata table
    """

    def __init__(self, table_name, sql_conn_id='airflow_default'):
        self.table_name = table_name
        self.sql_conn_id = sql_conn_id

    def delete(self, key, path):
        """
        Deletes outdated 
        """
        db = _get_sql_hook(self.sql_conn_id)
        sql = """\
        DELETE FROM
            {table}
        WHERE
            type = '{key}'
            AND path = '{path}'
        ;
        """.format(table=self.table_name, key=key, path=path)
        logging.info("Executing SQL: " + sql)
        db.run(sql)

    def get_records(self, filters=None, limit=1000):
        """
        Returns records in a list
        """
        db = _get_sql_hook(self.sql_conn_id)
        table = self.table_name
        filters = filters or []
        where = ' AND '.join(filters)
        sql = """\
        SELECT
            *
        FROM
            {table}
        """
        if where:
            sql += """\
            WHERE
                {where}
            """
        sql += """\
        LIMIT
            {limit}
        ;
        """
        return db.get_pandas_df(sql.format(**locals()))

    def insert_rows(self, rows):
        """
        Returns the underlying hook
        """
        db = _get_sql_hook(self.sql_conn_id)
        db.insert_rows(self.table_name, rows)

    def create(self, drop=False):
        """
        Creates the metadata table
        """
        db = _get_sql_hook(self.sql_conn_id)
        if drop:
            sql = "DROP TABLE IF EXISTS {};".format(self.table_name)
            logging.info("Executing SQL: \n" + sql)
            db.run(sql)
        sql = """\
        CREATE TABLE {table} (
            type    CHAR(32) NOT NULL,
            path    TEXT NOT NULL,
            stat    CHAR(64),
            val     FLOAT,
            ts      INT
        );
        """.format(table=self.table_name)
        logging.info("Executing SQL: \n" + sql)
        db.run(sql)


class Reporter:

    """
    Reports on the status of data
    """

    def __init__(self, args):
        pass

    def get_updated_timestamps(self):
        """
        Gets the metadata
        """
        raise NotImplementedError

    def __map_path_to_ts(self, obj):
        """
        Returns updated at times
        """
        updated_at = {}
        for item in obj.get_updated_timestamps():
            updated_at[item['path']] = item['ts']
        return updated_at

    def compare_to(self, other):
        """
        Compares two sets of data and determines what to update
        """
        self_updated_at = self.__map_path_to_ts(self)
        other_updated_at = self.__map_path_to_ts(other)
        update = []
        for item in other.get_updated_timestamps():
            # items that don't yet exist
            if item['path'] not in self_updated_at:
                update.append(item['path'])
            # items that are stale
            elif item['ts'] > self_updated_at[item['path']]:
                update.append(item['path'])
        delete = []
        for item in self.get_updated_timestamps():
            if item['path'] not in other_updated_at:
                delete.append(item['path'])
        return update, delete


class MetadataReporter(Reporter):

    """
    Interface to the metadata table
    """

    def __init__(
            self, table_name, key, path,
            sql_conn_id='airflow_default'):
        self.table_name = table_name
        self.sql_conn_id = sql_conn_id
        self.key = key
        self.path = path

    def get_updated_timestamps(self):
        """
        """
        db = _get_sql_hook(self.sql_conn_id)
        sql = """\
        SELECT
              type
            , path
            , stat
            , ts
        FROM
            {table}
        WHERE
            type LIKE '{key}'
            AND path LIKE '{path}'
        ;
        """.format(table=self.table_name, key=self.key, path=self.path)
        logging.info("Executing SQL: \n" + sql)
        data = db.get_records(sql)
        return [{'path': item[1], 'ts': item[3]} for item in data]


class LocalFsReporter(Reporter):

    def __init__(self, path):
        self.path = path

    def get_updated_timestamps(self):
        """
        Get timestamps for files
        """
        data = []
        for item in glob.glob(self.path):
            try:
                data.append({'path': item, 'ts': os.stat(item)[8]})
            except:
                logging.error("Failed to stat {}.".format(item))
                pass
        return data


class HdfsReporter(Reporter):

    """
    Reports the updated at timestamps for data in hdfs
    """

    def __init__(self, path, hdfs_conn_id='hdfs_default'):
        self.path = path
        self.hdfs_conn_id = hdfs_conn_id

    def get_updated_timestamps(self):
        """
        Get the updated_at timestamp of each file
        """
        try:
            hdfs = HDFSHook(self.hdfs_conn_id).get_conn()
            data = [{'path': item['path'], 'ts':item['modification_time']} for
                    item in hdfs.ls([self.path])]
            return data
        except:
            logging.error("HdfsReporter: Failed to stat {}".format(self.path))
            return []


class HiveReporter(Reporter):

    def __init__(self, path, metastore_mysql_conn_id='metastore_mysql'):
        self.metastore_conn_id = metastore_mysql_conn_id
        self.path = path
        self.db = 'default'
        parts = self.path.split('.')
        self.table_name = parts[0]
        self.partition_expr = None
        if len(parts) == 2:
            self.db, self.table_name = parts
            parts = self.table_name.split('/')
            if len(parts) == 2:
                self.table_name = parts[0]
                self.partition_expr = '/'.join(parts[1:])

    def get_updated_timestamps(self):
        """
        Query the metastore for partition landing times
        """
        table_name = self.table_name
        partition_expr = self.partition_expr
        db = self.db
        sql = """\
        SELECT
        """
        if partition_expr:
            sql += """
              p.PART_NAME
            , p.CREATE_TIME
            """
        else:
            sql += """
              NULL
            , t.CREATE_TIME
            """
        sql += """
        FROM (
            SELECT
                n.TBL_ID
                , n.TBL_NAME
                , n.CREATE_TIME
            FROM (
                SELECT
                    TBL_NAME
                    , TBL_ID
                    , DB_ID
                    , CREATE_TIME
                FROM
                    metastore.TBLS
                WHERE
                    TBL_NAME = '{table_name}'
            ) n
            INNER JOIN (
                SELECT
                    DB_ID
                FROM
                    metastore.DBS
                WHERE
                    NAME = '{db}'
            ) d
            ON
                n.DB_ID = d.DB_ID
        ) t
        """
        if partition_expr:
            sql += """
        INNER JOIN
            metastore.PARTITIONS p
        ON
            p.TBL_ID = t.TBL_ID
        WHERE
            p.PART_NAME LIKE '{partition_expr}'
        ;
        """
        sql = sql.format(**locals())
        logging.info("Querying metastore: " + sql)
        try:
            metastore = MySqlHook(mysql_conn_id=self.metastore_conn_id)
            rows = metastore.get_records(sql)
            data = []
            for row in rows:
                path = '{}.{}'.format(self.db, self.table_name)
                if partition_expr:
                    path += '/{}'.format(row[0])
                updated_at = row[1]
                data.append({'path': path, 'ts': updated_at})
            return data
        except:
            logging.error("HiveReporter: Failed to stat {}".format(self.path))
            return []


class MySqlReporter(Reporter):
    # TODO
    pass


class S3Reporter(Reporter):
    # TODO
    pass
