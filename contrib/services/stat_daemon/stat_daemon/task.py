import imp
import json
import logging
import os
import time

from util import (MetadataTable, MetadataReporter,
                  LocalFsReporter, HdfsReporter, HiveReporter,
                  require_args, update_args)


class TaskList(object):

    def __init__(self, args):
        require_args(args, {'type', 'path'})
        metadata = MetadataReporter(
            table_name=args.dest,
            path=args.path,
            sql_conn_id=args.sql_conn_id)
        self.update, self.delete = [], []
        reporter = None
        if args.type == 'hive':
            reporter = HiveReporter(
                path=args.path,
                metastore_mysql_conn_id=args.metastore_mysql_conn_id)
        elif args.type == 'hdfs':
            reporter = HdfsReporter(
                path=args.path,
                hdfs_conn_id=args.hdfs_conn_id)
        elif args.type == 'local_fs':
            reporter = LocalFsReporter(path=args.path)
        else:
            logging.error("Unrecognized type: {}".format(args.type))
            return
        self.update, self.delete = metadata.compare_to(reporter)

    def get_update_tasks(self):
        return self.update

    def get_delete_tasks(self):
        return self.delete


class TaskLoader(object):

    def __init__(self, roots):
        self.tasks = []
        if type(roots) == str:
            roots = [roots]
        for root in roots:
            for dirpath, dirnames, filenames in os.walk(root):
                for idx, item in enumerate(filenames):
                    try:
                        path = '{dirpath}/{item}'.format(**locals())
                        logging.info("Loading taskfile {}.".format(path))
                        taskfile = imp.load_source('task_{}'.format(idx), path)
                        for var_name in dir(taskfile):
                            var = getattr(taskfile, var_name)
                            if isinstance(var, StatsQueue):
                                if var.tasks:
                                    logging.info(
                                        "Loaded {} tasks from {}.".format(
                                            len(var.tasks), item))
                                    self.tasks += var.tasks
                    except Exception as e:
                        logging.error(
                            "Failed to load taskfile {}.".format(item))
                        logging.error(e)

    def get_tasks(self):
        """
        """
        return self.tasks


class TaskRunner(object):

    def __init__(self, executor, args):
        self.n_tasks = 0
        self.executor = executor
        self.args = args
        self.args_ = args

    def run_one(self):
        """
        Run one iteration of update stats
        """
        args = self.args_
        require_args(args, {'type', 'path'})
        tasks = TaskList(args)
        tbl = MetadataTable(table_name=args.dest,
                            sql_conn_id=args.sql_conn_id)
        for item in tasks.delete:
            logging.info("Deleting outdated item: {}".format(item))
            tbl.delete_records(item)
        for item in tasks.update:
            key = '{}-{}'.format(args.type, item)
            cmd = "stat_daemon update "
            for k, v in vars(args).iteritems():
                if k == 'path':
                    cmd += ' \\\n --path={}'.format(item)
                elif v and k not in {'func', 'maxjobs', 'sleep',
                                     'taskfolder', 'executor'}:
                    cmd += ' \\\n --{}={}'.format(k, v)
            self.executor.execute_async(key, cmd)
            self.n_tasks += 1
            if self.n_tasks >= args.maxjobs:
                return

    def run(self):
        """
        Enter the main loop
        """
        while 1:
            self.args_ = self.args
            self.executor.start()
            if self.args.taskfolder:
                loader = TaskLoader(self.args.taskfolder)
                for task in loader.get_tasks():
                    self.args_ = update_args(self.args, task)
                    self.run_one()
                    if self.n_tasks >= self.args.maxjobs:
                        break
            else:
                self.run_one()
            self.executor.end()
            if self.args.func.__name__ == 'test':
                break
            else:
                logging.info("Sleeping for {} seconds".format(self.args.sleep))
                time.sleep(self.args.sleep)
                self.n_tasks = 0


class StatsQueue(object):

    def __init__(self):
        self.tasks = []

    def add_task(self, task):
        self.tasks.append(task)

    def get_tasks(self):
        return self.tasks


class Stats(object):

    def __init__(self, type, path,
                 plugin=None, plugin_args=None, queue=None):
        self.type = type
        self.path = path
        self.plugin = plugin
        self.plugin_args = plugin_args
        if self.plugin_args:
            self.plugin_args = "'{}'".format(json.dumps(self.plugin_args))
        if queue:
            queue.add_task(self)

    def __str__(self):
        s = ''
        for k, v in self.__dict__.iteritems():
            if v:
                s += ' --{} {}'.format(k, v)
        return s


class HiveStats(Stats):

    def __init__(self, path, plugin=None, plugin_args=None,
                 queue=None, presto_conn_id=None):
        Stats.__init__(self, 'hive', path, plugin, plugin_args, queue)


class HdfsStats(Stats):

    def __init__(self, path, plugin=None, plugin_args=None,
                 queue=None, hdfs_conn_id=None):
        Stats.__init__(self, 'hdfs', path, plugin, plugin_args, queue)
