import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class StageCopyOperator(BaseOperator):

    def __init__(
            self,
            stage_table,
            staging_schema,
            redshift_conn_id,
            target_schema,
            target_table = str(),
            insert_columns = str(),
            select_columns = str(),
            primary_key = str(),
            sort_key = str(),
            run_dates = list(),
            *args, **kwargs):
        super(StageCopyOperator, self).__init__(*args, **kwargs)
        self.stage_table = stage_table
        self.staging_schema = staging_schema
        self.target_table = target_table
        self.target_schema = target_schema
        self.insert_columns = insert_columns
        self.select_columns = select_columns
        self.primary_key = primary_key
        self.sort_key = sort_key
        self.redshift_conn_id = redshift_conn_id
        self.run_dates = run_dates

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for run_date in self.run_dates:
            # Insert new data into target table from stage table
            insert_query = f"""
                INSERT INTO {self.target_schema}.{self.target_table} ({self.insert_columns})
                WITH dupe_counter_cte as (
                    SELECT
                        {self.insert_columns},
                        row_number() OVER (PARTITION BY {self.primary_key} ORDER BY {self.sort_key}) AS row_number
                    FROM {self.staging_schema}.{self.stage_table}
                ), deduping_cte as (
                    SELECT {self.insert_columns}
                    FROM dupe_counter_cte
                    WHERE row_number = 1
                )
                SELECT {self.select_columns}
                FROM deduping_cte s
                LEFT OUTER JOIN {self.target_schema}.{self.target_table} t ON s.{self.primary_key} = t.{self.primary_key}
                WHERE t.{self.primary_key} IS NULL;
                """

            logging.info(f"Processing {run_date['year']}-{run_date['month']}-{run_date['day']}")

            
            logging.info(f"Executing Redshift INSERT command for {self.target_schema}.{self.target_table}")
            
            redshift_hook.run(insert_query, True)

            logging.info(f"INSERT command complete for table {self.target_schema}.{self.target_table}")
            