from airflow.plugins_manager import AirflowPlugin



class AWSOperatorsPlugin(AirflowPlugin):
    name = "Airflow operators plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
