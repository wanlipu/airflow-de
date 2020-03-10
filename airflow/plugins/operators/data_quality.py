from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                tables=None,
                sql_template="",
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.sql_template = sql_template
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            sql_command = self.sql_template.format(table)
            self.log.info(f"Executing {sql_command} ...")
        
            records = redshift_hook.get_records(sql_command)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check on table {table} failed with no result returned")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check on table {table} failed with 0 row")

            self.log.info(f"Data quality check on table {table} passed with {records[0][0]} records")