import logging

import pymysql


class MySqlRepository(object):

    def __init__(self, db_config):
        self.db_connection = pymysql.connect(**db_config)
        self.db_cursor = self.db_connection.cursor(pymysql.cursors.DictCursor)

    def fetch_one_record(self, query):
        """
        Repo function to fetch one record
        :param query: query to get data
        :return: list of rows
        """
        self.db_connection.ping(reconnect=True)
        self.db_cursor.execute(
            query)
        records = self.db_cursor.fetchone()
        self.db_connection.commit()
        return records

    def insert_and_return_id(self, query):
        """
        Repo function to update and return ID of last row
        :param query: query to get data
        :return: ID of last updated row
        """
        self.db_connection.ping(reconnect=True)
        self.db_cursor.execute(
            query
        )
        self.db_connection.commit()
        last_id = self.db_cursor.lastrowid
        return last_id

    def execute_query(self, query):
        """
        Repo function to execute one query
        :param query: query to get data
        :return: none
        """
        logging.getLogger("MYSQL_REPO").info(query)
        self.db_connection.ping(reconnect=True)
        self.db_cursor.execute(
            query)
        self.db_connection.commit()

    def execute_multiple_query(self, query):
        """
        Repo function to execute many queries
        :param query: query to get data
        :return: none
        """
        self.db_connection.ping(reconnect=True)
        self.db_cursor.execute(query, multi=True)
        self.db_connection.commit()
