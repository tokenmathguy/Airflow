import copy
import imp
import json
import logging
import os
import time

from util import (MetadataTable, MetadataReporter,
                  LocalFsReporter, HdfsReporter, HiveReporter,
                  require_args, update_args)


class TaskList(object):

    """
    Determines what metadata needs to be updated
    """

    def __init__(self, args):
        require_args(args, {'type', 'path'})
        print args.path
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

    """
    Loads metadata upate tasks from one or more job creation scripts

    :param roots: path to folder containing job creation scripts
    :type roots: str    
    """

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
        :return: list of Stat
        :rtype: list
        """
        return self.tasks


class TaskRunner(object):

    """
    Loads metadata upate tasks from one or more job creation scripts
    """

    def __init__(self, executor, args):
        """
        :param executor: the executor to run with
        :type executor: Executor
        """
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
        excluded_args = {'func', 'maxjobs', 'sleep', 'taskfolder',
                         'parallelism', 'executor'}
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
                elif v and k not in excluded_args:
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

    """
    A simple container to hold Stats objects.  A stats queue must be created
    during a job creation script.
    """

    def __init__(self):
        self.tasks = []

    def add_task(self, task):
        self.tasks.append(task)

    def get_tasks(self):
        return self.tasks


class StatsTask(object):

    """
    Stats collection instruction.  This class simply helps format
    a stat_daemon update command.
    """

    def __init__(self, **kwargs):
        """
        """        
        if not kwargs.get('type'):
            logging.error("Stats: argument 'type' required")
        if not kwargs.get('path'):
            logging.error("Stats: argument 'path' required")
        if kwargs.get('queue'):
            kwargs.get('queue').add_task(self)
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def __str__(self):
        s = ''
        for k, v in self.__dict__.iteritems():
            if v:
                s += ' --{} {}'.format(k, v)
        return s


class HiveStats(StatsTask):

    """
    Stats collection for hive table
    """

    def __init__(self, **kwargs):
        """
        """
        kwargs['type'] = 'hive'
        StatsTask.__init__(self, **kwargs)


class HdfsStats(StatsTask):

    """
    Stats collection for hdfs path
    """

    def __init__(self, **kwargs):
        """
        """
        kwargs['type'] = 'hdfs'
        StatsTask.__init__(self, **kwargs)

