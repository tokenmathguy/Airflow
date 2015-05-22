#!/usr/bin/env python
import logging
import time
from airflow.hooks import HDFSHook

def get_rows(args):
    """
    Default stats updater for hdfs
    """
    hdfs = HDFSHook(args.hdfs_conn_id).get_conn()
    try:
        rows = []
        for item in hdfs.du([args.path],
                include_toplevel=True, include_children=False):
            path = 'hdfs:' + args.path
            row = [path, 'size', item['length'], int(time.time())]
            rows.append(row)
            break
        count = 0
        if item:
            count = sum(1 for _ in hdfs.ls([args.path]))
        row = [path, 'item_count', count, int(time.time())]
        rows.append(row)
        return rows
    except Exception as e:
        logging.error("Failed to stat: {}".format(args.path))
        logging.error(e)
