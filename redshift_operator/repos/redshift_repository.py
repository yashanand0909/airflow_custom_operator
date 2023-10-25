import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook


class RedshiftRepository(object):

    def __init__(self, redshift_conn_id):
        self.pg_hook = PostgresHook(redshift_conn_id)

    def execute_query(self, query, job_name, autocommit=False):
        """
        This function is to execute any query provided in parameter
        :param autocommit: to make call transactoinal
        :param job_name: Name of the job running requesting to run this function
        :param query: Query to execute
        :return: none
        """
        logging.getLogger(job_name).info("Executing query: " + query)
        self.pg_hook.run(query, autocommit=autocommit)

    def get_records(self, query, job_name):
        """
        This function executes query provided in parameter and returns list of rows
        :param job_name: Name of the job running requesting to run this function
        :param query: Query to get records
        :return: List of rows
        """
        logging.getLogger(job_name).info("Getting records for query: " + query)
        return self.pg_hook.get_records(query)
