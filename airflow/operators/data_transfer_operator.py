import logging
import tempfile

from airflow.models import BaseOperator
from airflow.utils import apply_defaults

class DataTransferOperator(BaseOperator):
   """
   Moves data from a source system to a destination system

   """


   __mapper_args__ = {
       'polymorphic_identity': 'DataTransferOperator'
   }

   template_fields = [source_conn_cmd, destination_data_cmd]
   template_ext = []
   ui_color = '#fff'

   @apply_defaults
   def __init__(
           self, source_conn_id,
           source_data_cmd,
           destination_conn_id,
           destination_data_cmd,
           *args, **kwargs):

       super(DataTransferOperator, self).__init__(*args, **kwargs)


class S3toHiveOperator(DataTransferOperator):
   """
   Moves data from a source system to a destination system

   """


   __mapper_args__ = {
       'polymorphic_identity': 'S3toHiveOperator'
   }

   template_fields = [source_conn_cmd, destination_data_cmd]
   template_ext = ['hql',]
   ui_color = '#fff'

   @apply_defaults
   def __init__(
           self, s3_conn_id='S3_default',
           key,
           hive_conn_id='hive_default',
           hql,
           *args, **kwargs):

       self.source_conn_id=s3_conn_id
       self.source_data_cmd=key
       self.destination_conn_id=hive_conn_id
       self.destination_data_cmd=hql

       super(S3toHiveOperator, self).__init__(*args, **kwargs)


