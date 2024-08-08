import logging
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class TruncateTableOperator(BaseOperator):

    def __init__(self, schema, table, redshift_conn_id, *args, **kwargs):
        super(TruncateTableOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #  Truncating  stage table
        truncate_query = f"""TRUNCATE TABLE {self.schema}.{self.table}"""

        logging.info(f"Executing Redshift TRUNCATE command for {self.schema}.{self.table}")

        redshift_hook.run(truncate_query, True)

        logging.info(f"TRUNCATE command complete for table {self.schema}.{self.table}")
            