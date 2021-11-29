from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.jdbc_hook import JdbcHook

import pandas as pd


### Default args DAGs ###
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


### Instantiate DAGs ###
dag = DAG('test_test_v-1.0', description='Test connection to Hive', default_args = default_args, schedule_interval = None,
          params={
              'labels': {
                  'env': 'test'
              }
          }
        )


### Create Connection JDBC
connHiveJDBC = JdbcHook(jdbc_conn_id='HiveJDBC')


## Mengambil data dari HIVE dan di Convert ke list
def fetchDataFromTable(query,conn):
    results = conn.get_records(sql=query)
    return results


def get_importantword(conn):
    query = "SELECT * FROM important_word LIMIT 5"
    return fetchDataFromTable(query, conn)


def getDataImportantWord(**kwargs):
    task_instance   = kwargs['task_instance']
    result = get_importantword(connHiveJDBC)
    result = pd.DataFrame(result)
    task_instance.xcom_push(key = 'dataImportantWord', value = result)
    return result


def printFail():
    print("Query tidak berjalan")


### Task ###
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = PythonOperator(
    task_id='getDataImportantWord',
    provide_context = True,
    python_callable = getDataImportantWord,
    dag = dag
)

t1 >> t2




