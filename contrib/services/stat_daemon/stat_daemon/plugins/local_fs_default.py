#!/usr/bin/env python
import glob
import logging
import os
import time

def get_rows(args):
    """
    Default stats updater for local_fs
    """
    try:
        host = os.uname()[1]
        size = os.stat(args.path)[6]
        path = '{}:{}'.format(host, args.path)
        row1 = [path, 'size', size, int(time.time())]
        count = sum(1 for _ in glob.glob(args.path))
        row2 = [path, 'item_count', count, int(time.time())]
        return [row1, row2]
    except:
        logging.error("Failed to stat: {}".format(args.path))
