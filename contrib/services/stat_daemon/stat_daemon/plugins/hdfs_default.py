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
            row = ['hdfs', args.path, 'size', item['length'], int(time.time())]
            rows.append(row)
            break
        return rows
    except:
        logging.error("Failed to stat: {}".format(args.path))
