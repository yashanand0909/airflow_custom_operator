import logging
from functools import wraps

from custom_operator.constants import constant_queries
from custom_operator.enums.node_status_enum import NodeStatus

job_name = 'maintain_node_runs_job'


def maintain_node_run(func):
    """
    Decorator to add and update node runs
    :param func: Function to apply decorator
    :return: none
    """

    @wraps(func)
    def wrapper_to_add_node_execution(*args, **kwargs):
        try:
            run_id = create_node_runs(*args, **kwargs)
        except Exception as ex:
            error = "Error Occurred while creating node run entry for node - {0}".format(
                kwargs['node_record']['node_id'])
            logging.getLogger(job_name).error(error)
            raise ex
        try:
            func(*args, **kwargs)
            is_executed = False
            attempts = 0
            while (not is_executed and attempts < 3):
                try:
                    logging.getLogger(job_name).info("Attempt number {} to update mysql of success".format(attempts))
                    update_node_run_status(run_id, NodeStatus.SUCCESS.value, "Task ran successfully",
                                           mysql=kwargs['mysql'])
                    is_executed = True
                except Exception as ex:
                    if attempts == 2:
                        logging.getLogger(job_name).error("All retries exhausted to update mysql for success")
                        raise ex
                attempts += 1

        except Exception as ex:
            error = "Error Occurred while running execution - {0}  of node id - {1} with error - {2}" \
                .format(kwargs['dag_run_id'], kwargs['node_record']['node_id'], str(ex))
            logging.getLogger(job_name).error(error)
            update_node_run_status(run_id, NodeStatus.FAILED.value, error, mysql=kwargs['mysql'])
            raise ex

    return wrapper_to_add_node_execution


def create_node_runs(*args, **kwargs):
    node_id = kwargs['node_record']['node_id']
    node_name = kwargs['node_record']['node_name']
    job_id = kwargs['node_record']['job_id']
    updated_by = kwargs['node_record']['updated_by']
    metadata = kwargs['node_record']['metadata'].replace('\\', '\\\\\\')  # iska encoding dekh lena
    query = kwargs['query']
    destination_config = kwargs['node_record']['destination_config']
    dag_run_id = kwargs['dag_run_id']
    mysql = kwargs['mysql']
    logging.getLogger(job_name).info("Create node runs called for node id - {}".format(node_id))
    return mysql.insert_and_return_id(
        constant_queries.insert_node_run_query.format(node_id, node_name, job_id, updated_by, dag_run_id,
                                                      query.replace("'", " "), 'Task Execution has started !!!',
                                                      destination_config, NodeStatus.RUNNING.value, metadata))


def update_node_run_status(run_id, status, execution_summary, mysql):
    logging.getLogger(job_name).info("update node runs status called for node id - {}".format(run_id))
    mysql.execute_query(constant_queries.update_node_run_status.format(status, execution_summary, run_id))
