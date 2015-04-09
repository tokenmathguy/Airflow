from collections import OrderedDict
import csv
import logging
from tempfile import NamedTemporaryFile
import boto

from airflow.hooks import HiveCliHook, S3Hook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class S3ToHiveTransfer(BaseOperator):
    """
    Moves data from S3 to Hive. The operator downloads a file from S3,
    stores the file locally before loading it into a Hive table.
    If the ``create`` or ``recreate`` arguments are set to ``True``,
    a ``CREATE TABLE`` and ``DROP TABLE`` statements are generated.
    Hive data types are inferred from the cursors's metadata from.

    Note that the table genearted in Hive uses ``STORED AS textfile``
    which isn't the most efficient serialization format. If a
    large amount of data is loaded and/or if the tables gets
    queried considerably, you may want to use this operator only to
    stage the data into a temporary table before loading it into its
    final destination using a ``HiveOperator``.

    :param key: The key to be retrieved from S3
    :type key: str
    :param hive_table: target Hive table, use dot notation to target a
        specific database
    :type hive_table: str
    :param create: whether to create the table if it doesn't exist
    :type create: bool
    :param recreate: whether to drop and recreate the table at every
        execution
    :type recreate: bool
    :param partition: target partition as a dict of partition columns
        and values
    :type partition: dict
    :param delimiter: field delimiter in the file
    :type delimiter: str
    :param s3_conn_id: source s3 connection
    :type s3_conn_id: str
    :param hive_conn_id: desctination hive connection
    :type hive_conn_id: str
    """

    __mapper_args__ = {
        'polymorphic_identity': 'S3ToHiveOperator'
    }
    template_fields = ('key', 'partition', 'hive_table')
    template_ext = ()
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            key,
            field_dict,
            hive_table,
            delimiter=',',
            create=True,
            recreate=False,
            partition=None,
            headers=True,
            s3_conn_id='s3_default',
            hive_cli_conn_id='hive_cli_default',
            *args, **kwargs):
        super(S3ToHiveTransfer, self).__init__(*args, **kwargs)
        self.key = key
        self.hive_table = hive_table
        self.partition = partition
        self.create = create
        self.recreate = recreate
        self.delimiter = source_delimiter
        self.headers = headers
        self.hive = HiveCliHook(hive_cli_conn_id=hive_cli_conn_id)
        self.s3 = S3Hook(s3_conn_id=mysql_conn_id)

    def execute(self, context):
        logging.info("Downloading S3 file")
        conn = self.s3.get_conn()
        if not conn.check_for_key(self.key):
            raise Exception("The key {0} does not exists".format(self.key))
        s3_key_object = conn.get_key(self.key)
        with NamedTemporaryFile("w") as f:
            logging.info("Dumping S3 file {0} contents to local file {1}".format(self.key, f.name))
            s3_key_object.get_contents_to_file(f)
            f.flush()
            conn.close()
            logging.info("Loading file into Hive")
            self.hive.load_file(
                f.name,
                self.hive_table,
                field_dict=field_dict,
                create=self.create,
                partition=self.partition,
                delimiter=self.delimiter,
                recreate=self.recreate)
