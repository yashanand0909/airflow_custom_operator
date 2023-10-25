import json
import logging

import requests

import custom_operator.constants.constant_values as constants


class HttpUtils:
    def __init__(self):
        pass

    def post(self, body, job_name, url, headers):
        try:
            payload = json.dumps(body)
            response = requests.request("POST", url, headers=headers, data=payload)
            logging.getLogger(job_name).info("Response code for hit: {} ".format(response.status_code))
            if response.status_code == 200:
                return response.json(), True
            else:
                raise Exception(response.json())
        except Exception as ex:
            logging.getLogger(job_name).info(" API hit failed with message: '{0}' ".format(str(ex)))
            raise ex

    def get(self, job_name, url, headers, body=''):
        try:
            payload = json.dumps(body)
            response = requests.request("GET", url, headers=headers, data=payload)
            logging.getLogger(job_name).info("Response code for hit: {0} ".format(str(response.status_code)))
            return response.json()
        except Exception as ex:
            logging.getLogger(job_name).info(" API hit failed with message: {0} ".format(str(ex)))
            raise ex

    def patch(self, body, job_name, url, headers):
        try:
            payload = json.dumps(body)
            response = requests.request("GET", url, headers=headers, data=payload)
            logging.getLogger(job_name).info("Response code for hit: {0} ".format(response.status_code))
            if response.status_code == 200 or 201:
                return True
            else:
                return False
        except Exception as ex:
            logging.getLogger(job_name).info(" API hit failed with message: {0} ".format(str(ex)))
            raise ex
