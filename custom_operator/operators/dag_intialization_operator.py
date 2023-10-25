import logging

from airflow.models import BaseOperator

from custom_operator.constants import constant_queries
from custom_operator.enums.node_status_enum import NodeStatus
from custom_operator.repos.mysql_repository import MySqlRepository
from custom_operator.utilities.config_util import ConfigManager
from custom_operator.utilities.notification_util import Notification

job_name = 'DagInitialization JOB'

class DagInitializationOperator(BaseOperator):

    def __init__(self, db_conn_id, job_id, *args, **kwargs):
        super(DagInitializationOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.job_id = job_id

    def execute(self, context):
        """
        Executes logic of DagInitializationOperator operator
        :param context: Context of airflow
        :return:
        """
        logging.getLogger(job_name).info("Execution started for job - {}".format(self.job_id))
        db_conn = ConfigManager().get_connection_config(self.db_conn_id)
        mysql = MySqlRepository(db_conn)
        try:
            dag_record = mysql.fetch_one_record(constant_queries.fetch_dag_query.format(self.job_id))
            if dag_record is None:
                raise Exception("Job not found for the given job Id ")
            logging.getLogger(job_name).info("Updating job status to RUNNING for job - {}".format(self.job_id))
            mysql.execute_query(constant_queries.update_dag_status.format(NodeStatus.RUNNING.value,
                                                                          self.job_id))

        except Exception as ex:
            logging.getLogger(job_name).error("DAG initialization failed for dag_id {0} with error - {1}".format(self.job_id, str(ex)))
            raise ex
