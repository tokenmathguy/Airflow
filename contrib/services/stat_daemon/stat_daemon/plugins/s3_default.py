#!/usr/bin/env python
import logging
import time
from airflow.hooks import S3Hook

def get_rows(args):
    """
    Default stats updater for hdfs
    """
    s3 = S3Hook(args.s3_conn_id).get_conn()
    try:
        key = s3.lookup(args.path)
        row = ['s3', args.path, 'size', key.size, int(time.time())]
        return [row]
    except:
        logging.error("Failed to process: {}".format(args.path))
