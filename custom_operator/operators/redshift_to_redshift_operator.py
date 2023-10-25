import json
import logging

from airflow.models import BaseOperator

from custom_operator.constants import constant_queries, constant_values
from custom_operator.decorators.node_run_decorator import \
    maintain_node_run
from custom_operator.repos.mysql_repository import MySqlRepository
from custom_operator.repos.redshift_repository import \
    RedshiftRepository
from custom_operator.utilities.config_util import ConfigManager
from custom_operator.utilities.operator_util import *

job_name = 'ExecuteRedshiftQueryOperator JOB'


class RedshiftToRedshiftOperator(BaseOperator):
    ui_color = "#87CEEB"

    def __init__(self, redshift_conn_id, redshift_master_conn_id, db_conn_id, task_to_run_id, *args, **kwargs):
        super(RedshiftToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn = RedshiftRepository(redshift_conn_id)
        self.redshift_master_conn = RedshiftRepository(redshift_master_conn_id)
        self.task_to_run_id = task_to_run_id
        self.db_conn_id = db_conn_id


    def __check_table_count(self, table_name):
        """
        This will check if the table has more than 0 count to proceed
        :param table_name: Name of the table to check the count of
        :return: boolean
        """
        query = constant_queries.select_count_query.format(table_name)
        count_return = self.redshift_conn.get_records(query, job_name)
        return count_return[0][0] > 0


    def __update_is_edited(self, mysql, node_id):
        """
        :param mysql: mysql connection to update data to
        :param node_id: node id to update is edited for
        :return: None
        """
        logging.getLogger(job_name).info("updating is edited column for node - {}".format(node_id))
        mysql.execute_query(constant_queries.update_is_edited.format(0, node_id))

    def __alter_main_table(self, columns_to_add, columns_to_delete, table_name):
        """
        Performs alter commands on table according to add_columns and delete_columns
        :param columns_to_add: Dict of columns to add to table
        :param columns_to_delete: Dict of columns to delete from table
        :param table_name: Name of table to alter
        :return: none
        """
        logging.getLogger(job_name).info("Altering table - {}".format(table_name))
        query = ''
        for key, value in columns_to_delete.items():
            query = query + constant_queries.drop_column_query.format(table_name, key) + ';'
        for key, value in columns_to_add.items():
            query = query + constant_queries.add_column_query.format(table_name, key, value, 'NULL') + ';'
        print(query)
        self.redshift_conn.execute_query(query, job_name)

    def __modify_main_table(self, columns_to_alter, table_name):
        """
        Execute all modify queries for the particular run
        :param columns_to_alter: dict of columns to modify
        :param table_name: name of table to perform action on
        :return: none
        """
        for key, value in columns_to_alter.items():
            self.redshift_conn.execute_query(constant_queries.modify_column_query.format(table_name, key, value)
                                             + ';', job_name, True)

    def __truncate_and_insert(self, query, full_table_name):
        """
        Truncates table provided in parameter
        :param full_table_name: Name of table to truncate
        :return: none
        """
        logging.getLogger(job_name).info("truncate and insert called for table - {}".format(full_table_name))
        truncate_query = constant_queries.delete_all_rows.format(full_table_name) + ';'
        truncate_query = truncate_query + (constant_queries.insert_into_query.format(full_table_name, query)) + ';'
        self.redshift_conn.execute_query(truncate_query, job_name)

    def __drop_table(self, full_table_name):
        """
        Drops table provided in parameter
        :param full_table_name:
        :return: none
        """
        logging.getLogger(job_name).info("drop_table function called for table {}".format(full_table_name))
        query = constant_queries.drop_table_query.format(full_table_name)
        self.redshift_conn.execute_query(query, job_name)

    def __create_table(self, query, table_name, is_limit, limit):
        """
        Creates table with and without name from the query provided on parameter
        :param query: Query for schema reference to create table
        :param table_name: Name of table to create
        :param is_limit: Boolean to check to apply limit
        :param limit: Limit
        :return: none
        """
        logging.getLogger(job_name).info("Create table running for table - {}".format(table_name))
        if is_limit:
            final_query = constant_queries.create_table_query.format(table_name,
                                                                     constant_queries.limit_select_query.format(query,
                                                                                                                limit))
        else:
            final_query = constant_queries.create_table_query.format(table_name, query)
        grant_access = constant_queries.grant_access_group_query.format(table_name, constant_values.redshift_report_group_name)
        self.redshift_conn.execute_query(final_query + ';' + grant_access + ';', job_name)

    def __insert_into_table(self, query, full_table_name):
        """
        Function to insert into table
        :param query: Query to get data to insert in new table
        :param full_table_name: Name of table to insert data into
        :return: none
        """
        logging.getLogger(job_name).info("Insert_into running for table - {}".format(full_table_name))
        self.redshift_conn.execute_query(constant_queries.insert_into_query.format(full_table_name, query), job_name)

    def __drop_and_insert_into(self, drop_query, insert_query, full_table_name):
        """
        Function to drop and insert into table
        :param drop_query: Query to delete from main table new table
        :insert_query: Query to get data from_temp table;
        :param full_table_name: Name of table to insert data into
        :return: none
        """
        logging.getLogger(job_name).info("drop and insert running for table - {}".format(full_table_name))
        query = drop_query + ";" + constant_queries.insert_into_query.format(full_table_name, insert_query) + ";"
        self.redshift_conn.execute_query(query, job_name)

    def __check_and_update_main_table(self, query, stage_table_name, final_table_name):
        """
        Checks new query and updates main table
        :param query: Updated query
        :param stage_table_name: Name of stage table
        :param final_table_name: Name of main table
        :return: none
        """
        logging.getLogger(job_name).info("{} table alter table started".format(final_table_name))
        self.__drop_table(stage_table_name)
        self.__create_table(query, stage_table_name, False, 0)
        new_columns_details = self.redshift_master_conn.get_records(constant_queries.column_details_query.format
                                                             (stage_table_name), job_name)
        old_columns_details = self.redshift_master_conn.get_records(constant_queries.column_details_query.format
                                                             (final_table_name), job_name)
        is_valid, comment = validate_query_change(new_columns_details, old_columns_details)
        print("validation comment - " + comment)
        if not is_valid:
            raise Exception("Query change validation change failed with message - " + comment)
        column_details_to_add, column_details_to_delete, column_details_to_alter = get_column_diff(new_columns_details, old_columns_details)
        if len(column_details_to_add) > 0 or len(column_details_to_delete) > 0:
            self.__alter_main_table(column_details_to_add, column_details_to_delete, final_table_name)
        if len(column_details_to_alter) > 0:
            self.__modify_main_table(column_details_to_alter, final_table_name)
        logging.getLogger(job_name).info("{} table altered successfully".format(final_table_name))

    @maintain_node_run
    def __truncate_load(self, stage_table_name, main_table_name, is_edited, is_table_created, node_record,
                        dag_run_id, mysql, query):
        """
        Truncate loads data to table
        :param query: Query to load data from
        :param stage_table_name: Name of stage table
        :param main_table_name: Name of main table
        :param is_edited: Boolean to get if the ETL is updated
        :return: none
        """
        logging.getLogger(job_name).info("Tuncate load started for table - {}".format(main_table_name))
        if not is_table_created:
            self.__create_table(query, main_table_name, True, 0)
        self.__check_and_update_main_table(query, stage_table_name, main_table_name)
        logging.getLogger(job_name).info("Stage table updated with latest data - {}".format(main_table_name))
        if not self.__check_table_count(stage_table_name):
            return
        select_query = constant_queries.select_all_query.format(stage_table_name)
        self.__truncate_and_insert(select_query, main_table_name)

    @maintain_node_run
    def __append_only_load(self, stage_table_name, main_table_name, is_edited, is_table_created, node_record,
                           dag_run_id, mysql, query):
        """
        Appends data to table
        :param query: Query to load data from
        :param stage_table_name: Name of stage table
        :param main_table_name: Name of main table
        :param is_edited: Boolean to get if the ETL is updated
        :return: none
        """
        logging.getLogger(job_name).info("Append load started for table - {}".format(main_table_name))
        if not is_table_created:
            self.__create_table(query, main_table_name, True, 0)
        self.__check_and_update_main_table(query, stage_table_name, main_table_name)
        self.__drop_table(stage_table_name)
        self.redshift_conn.execute_query(constant_queries.insert_into_query.format(main_table_name, query), job_name)

    @maintain_node_run
    def __incremental_load(self, stage_table_name, final_table_name, id_column_name, is_edited,
                           is_table_created, node_record, dag_run_id, mysql, query):
        """
        Loads data to table in incremental way
        :param query: Query to load data from
        :param stage_table_name: Name of stage table
        :param final_table_name: Name of main table
        :param id_column_name: name of ID column of table
        :param is_edited: Boolean to get if the ETL is updated
        :param is_table_created: boolean to tell if the table is already created
        :return: none
        """
        logging.getLogger(job_name).info("Incremental load started for table - {}".format(final_table_name))
        if not is_table_created:
            self.__create_table(query, final_table_name, True, 0)
        self.__check_and_update_main_table(query, stage_table_name, final_table_name)
        if not self.__check_table_count(stage_table_name):
            return
        drop_query = constant_queries.delete_rows_query_with_where.format(final_table_name, id_column_name,
                                                                          stage_table_name)
        logging.getLogger(job_name).info("Stage table updated with latest data - {}".format(final_table_name))
        query = constant_queries.select_all_query.format(stage_table_name)
        self.__drop_and_insert_into(drop_query, query, final_table_name)

    def execute(self, context):
        """
        Executes logic of ExecuteRedshiftQueryOperator operator
        :param context: Context of airflow
        :return: none
        """
        logging.getLogger(job_name).info("Execution started for task - {}".format(self.task_to_run_id))
        db_conn = ConfigManager().get_connection_config(self.db_conn_id)
        mysql = MySqlRepository(db_conn)
        node_record = mysql.fetch_one_record(constant_queries.fetch_task_query.format(self.task_to_run_id))
        destination = json.loads(node_record['destination_config'])
        run_type = destination['loadType']
        stage_table_name = constant_values.stage_schema + '.stage_' + destination['tableName']
        main_table_name = destination['schema'] + '.' + destination['tableName']
        comment_metadata = json.loads(node_record['metadata'])
        destination_table_info = self.redshift_master_conn.get_records(constant_queries.table_exists_query.format
                                                          (destination['schema'], destination['tableName']), job_name)
        query = get_parametrized_query(node_record['query'], json.loads(node_record['parameter_config']),
                                       context['data_interval_end'])
        try:
            if comment_metadata['query_comment'] is not None:
                query = comment_metadata['query_comment'] + query
        except Exception as e:
            logging.getLogger(job_name).info("Comments not found for node id - " + self.task_to_run_id)
        logging.getLogger(job_name).info("Request formed for task - {}".format(self.task_to_run_id))
        if run_type == 'TRUNCATELOAD':
            self.__truncate_load(stage_table_name, main_table_name,
                                 node_record['is_edited'], len(destination_table_info) > 0, node_record=node_record,
                                 dag_run_id=context['dag_run'].run_id, mysql=mysql, query=query)
            logging.getLogger(job_name).info("truncate load done for task Id - {}".format(self.task_to_run_id))
        elif run_type == 'APPEND':
            self.__append_only_load(stage_table_name,
                                    main_table_name,
                                    node_record['is_edited'], len(destination_table_info) > 0, node_record=node_record,
                                    dag_run_id=context['dag_run'].run_id, mysql=mysql, query=query)
            logging.getLogger(job_name).info("append load done for task Id - {}".format(self.task_to_run_id))
        elif run_type == "INCREMENTAL":
            self.__incremental_load(stage_table_name,
                                    main_table_name, destination['idColumns'],
                                    node_record['is_edited'], len(destination_table_info) > 0,
                                    node_record=node_record, dag_run_id=context['dag_run'].run_id,
                                    mysql=mysql, query=query)


class TestOperatorCheck(BaseOperator):

    def __init__(self, *args, **kwargs):
        super(TestOperatorCheck, self).__init__(*args, **kwargs)

    def execute(self, context):
        print('checking operator')
