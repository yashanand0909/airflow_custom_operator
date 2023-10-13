import logging

from airflow.models import Variable

import stitch_custom_operators.constants.constant_values as constants
from stitch_custom_operators.constants import constant_values
from stitch_custom_operators.utilities.http_utils import HttpUtils


class Notification:
    def __init__(self):
        pass

    def send_slack_alert(self, job_id, status, job_name):
        try:
            body = {"jobId": job_id, "status": str(status)}
            notification_url = Variable.get(constants.stitch_notification_service_url)
            HttpUtils().post(body, job_name, notification_url, constant_values.stitch_headers)
            logging.getLogger(job_name).info("Slack alert sent for dag_id {0} ", job_id)
        except Exception as ex:
            logging.getLogger(job_name).error(
                "Notification failed for dag_id {0} with error - {1}".format(job_id, str(ex)))
            raise ex
