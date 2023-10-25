from airflow.hooks.base import BaseHook


class ConfigManager:
    def __init__(self):
        pass

    def get_connection_config(self, conn_id):
        """
        Return connection config for Id
        :param conn_id: ID to get config for
        :return: config
        """
        connection_dict = BaseHook.get_connection(conn_id)
        host = connection_dict.host
        database = connection_dict.schema
        user = connection_dict.login
        password = connection_dict.password
        port = connection_dict.port
        db_conn_config = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': int(port)
        }
        return db_conn_config
