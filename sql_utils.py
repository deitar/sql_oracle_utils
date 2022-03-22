
import cx_Oracle

db_username = '[user]'
db_pwd = '[db_password]'
db_server = '[server_url or ip]'
db_service_id = '[service_id]'
db_port = '[port]'


# create connection object
def connect():
    conn_string = f'{db_username}/{db_pwd}@{db_server}:{db_port}/{db_service_id}'
    return cx_Oracle.connect(conn_string)


# for running operations like insert, uodate, delete, drop
def execute_nonquery(q):
    con = connect()
    cur = con.cursor()
    try:
        cur.execute(q)
    except Exception as e:
        print(e)
    finally:
        con.commit()
        cur.close()
        con.close()
       

# running select queries and returning data
def execute_query(q):
    con = connect()
    cur = con.cursor()
    try:
        cur.execute(q)
        ret = cur.fetchall()
    except Exception as e:
        print(e)
    finally:
        cur.close()
        con.close()
    return ret
   

# check if table exists under the schema/user set at the beginning of the code
def table_exists(table_name):
    q = f"select * from user_tables where table_name = upper('{table_name}')"
    ret = execute_query(q)
    return len(ret)>0
   
   
# drop table if table exists , then create table
def create_table_delete_if_exists(create_query, table_name):
    if table_exists(table_name):
        execute_nonquery(f'drop table {table_name} purge')
    execute_nonquery(create_query)


