from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                sql_source="",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_source = sql_source

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Deleting data from Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        sql_command = f"INSERT INTO {self.table} {self.sql_source};"
        self.log.info(f"Executing {sql_command} ...")
        redshift.run(sql_command)