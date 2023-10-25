########### Fetch Queries ########
fetch_task_query = '''select * from dstitch.node_master where node_id = {0}'''
fetch_dag_query = '''select * from dstitch.job_master where job_id = {0}'''

########### DDL & DML Queries ############
truncate_table_query = '''TRUNCATE TABLE {}'''
insert_into_query = '''INSERT INTO {0} ({1})'''
create_table_query = '''CREATE TABLE {0} as ({1})'''
drop_table_query = '''DROP TABLE IF EXISTS {0}'''
delete_rows_query_with_where = '''DELETE FROM {0} WHERE ({1}) in (SELECT {1} FROM {2})'''
select_all_query = '''SELECT * FROM {0}'''
select_query = '''SELECT {0} FROM {1}'''
limit_select_query = '''{0} LIMIT {1}'''
add_column_query = '''ALTER TABLE {0} ADD COLUMN {1} {2} DEFAULT {3}'''
drop_column_query = '''ALTER TABLE {0} DROP COLUMN {1}'''
delete_all_rows = '''DELETE FROM {0}'''
select_count_query = '''SELECT COUNT(1) FROM {0}'''
modify_column_query = '''ALTER TABLE {0} ALTER COLUMN {1} TYPE {2}'''

############## ACCESS QUERIES ###############
grant_access_group_query = '''GRANT SELECT ON {0} TO GROUP {1}'''
grant_access_user_query = '''GRANT SELECT {0} to {1}'''

############## STITCH QUERIES #############
insert_node_run_query = '''INSERT INTO dstitch.node_runs ( node_id, node_name, job_id, update_by, dag_run_id, executed_query,
                                    execution_summary, destination, node_status, metadata) value 
                                    ({0},'{1}',{2},'{3}','{4}','{5}','{6}','{7}','{8}','{9}')'''

update_node_run_status = '''UPDATE dstitch.node_runs SET node_status = '{0}', execution_summary = '{1}' 
                                    where nodes_run_id = {2}'''

update_dag_status = '''UPDATE dstitch.job_master SET job_status = '{0}' where job_id = {1}'''

update_is_edited = '''UPDATE dstitch.node_master SET is_edited = {0} where node_id = {1}'''

######### Structral Queries #############
column_details_query = '''select * from pg_get_cols('{0}') 
                            cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int)'''
table_exists_query = '''SELECT * FROM information_schema.tables
                                WHERE  table_schema = '{0}' AND    table_name   = '{1}' '''

