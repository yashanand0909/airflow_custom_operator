import re
from datetime import timedelta

import pandas as pd

from custom_operator.constants import constant_values


def get_column_diff(new_column_details, old_column_details):
    """
    Return dict of columns to add and dict of column to delete
    :param new_column_details: List of columns in new table
    :param old_column_details: List of columns in old table
    :return: Dict of columns to add and delete
    """
    column_details_to_add = {}
    column_details_to_delete = {}
    column_details_to_alter = {}
    new_columns_set = set()
    old_columns_set = set()
    for i in range(len(new_column_details)):
        print(new_column_details[i])
        new_columns_set.add(new_column_details[i][2])
    for i in range(len(old_column_details)):
        old_columns_set.add(old_column_details[i][2])
        if old_column_details[i][2] == new_column_details[i][2] and old_column_details[i][3] != new_column_details[i][
            3] and constant_values.character_string in old_column_details[i][3] and constant_values.character_string in \
                new_column_details[i][3]:
            old_value = int(re.findall(r'\(.*?\)', old_column_details[i][3])[0][1:-1])
            new_value = int(re.findall(r'\(.*?\)', new_column_details[i][3])[0][1:-1])
            if new_value > old_value:
                column_details_to_alter[new_column_details[i][2]] = new_column_details[i][3]
    columns_to_add = new_columns_set - old_columns_set
    columns_to_delete = old_columns_set - new_columns_set
    for i in range(len(new_column_details)):
        if new_column_details[i][2] in columns_to_add:
            column_details_to_add[new_column_details[i][2]] = new_column_details[i][3]
    for i in range(len(old_column_details)):
        if old_column_details[i][2] in columns_to_delete:
            column_details_to_delete[old_column_details[i][2]] = old_column_details[i][3]
    return column_details_to_add, column_details_to_delete, column_details_to_alter


def extract_date(parameter, execution_date):
    """
    Extract parameterized date
    :param parameter: Parameter to extract date for
    :param execution_date: Execution date
    :return: Date string
    """
    if str(parameter['type']) == 'DATE':
        parameter_config = parameter['config']
        delay_type = parameter_config['delayType']
        if delay_type == 'SECOND':
            execution_date = execution_date - timedelta(seconds=parameter_config['delayNumber'])
        elif delay_type == 'MINUTE':
            execution_date = execution_date - timedelta(minutes=parameter_config['delayNumber'])
        elif delay_type == 'HOUR':
            execution_date = execution_date - timedelta(hours=parameter_config['delayNumber'])
        elif delay_type == 'DAY':
            execution_date = execution_date - timedelta(days=parameter_config['delayNumber'])
        elif delay_type == 'WEEK':
            execution_date = execution_date - timedelta(weeks=parameter_config['delayNumber'])
        elif delay_type == 'MONTH':
            execution_date = execution_date - pd.DateOffset(months=parameter_config['delayNumber'])
        elif delay_type == 'YEAR':
            execution_date = execution_date - pd.DateOffset(months=parameter_config['delayNumber'] * 12)
    return execution_date


def get_parametrized_query(query, parameter_config, execution_date):
    """
    Return parameter replaced query
    :param query: Query to replace parameters
    :param parameter_config: Parameter config
    :param execution_date: Execution date
    :return: Query
    """
    for parameter in parameter_config['parametersList']:
        date_to_replace = extract_date(parameter, execution_date)
        query = query.replace('{' + parameter['parameterName'] + '}', date_to_replace.strftime(
            parameter['config']['dateFormat']))
    return query


def validate_query_change(new_column_details, old_column_details):
    print("validate query called")
    if len(new_column_details) >= len(old_column_details):
        for i in range(len(old_column_details)):
            if old_column_details[i][2] != new_column_details[i][2]:
                return False, "Column name mismatch found in new query!!! {0} has been changed to {1}" \
                    .format(old_column_details[i][2], new_column_details[i][2])
        return True, "All cool with query change"
    return False, "Column deletion not allowed in query change please check the query again!!"
