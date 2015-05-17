#!/usr/bin/env python
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
        row = ['local_fs', '{}:{}'.format(host, args.path), 
                'size', size, int(time.time())]
        return [row]
    except:
        logging.error("Failed to stat: {}".format(args.path))

