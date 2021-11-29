from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pymysql ## Koneksi ke mysql
from datetime import timedelta, date, datetime
import pandas as pd


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Import data center to docker mysql',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 26),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

### Initiate DAGs
dag = DAG(
    'Import_Datacenter_to_Docker-v1.3',
    default_args=default_args,
    description='A selection process important Word',
    schedule_interval= None,
)

ConfWarehouseDB = BaseHook.get_connection("Warehouse") ## Koneksi ke datacenter
ConfWarehouseDBDocker = BaseHook.get_connection("Warehouse-test") ### koneksi ke docker mysql

## Fungsi untu membuat koneksi ke database
def makeConnection(host, database, user='', password='',port=3306):
    return pymysql.connect(host=host,port=int(port),user=user,passwd=password,db=database,charset='utf8mb4')

## Mengambil data dari MySQL dan di Convert ke list
def fetchDataFromTable(query,conn):
    q_result = pd.read_sql_query(query, conn)
    return q_result

### execute query the way cursor normal commit to database
def getConnection(query, conn):
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    return


connWarehouseWhmcs  = makeConnection(ConfWarehouseDB.host, ConfWarehouseDB.schema, ConfWarehouseDB.login, ConfWarehouseDB.password, ConfWarehouseDB.port)
connWarehouseTestdb  = 'mysql+pymysql://root:admin@172.17.0.2:3306/testdb'


####################################################
##################  QUERY BLOCKS  ##################
####################################################


def get_livechatmessage(conn):
    query = "SELECT * FROM `livechatmessages`"
    return fetchDataFromTable(query, conn)

def get_stopword(conn):
    query = "SELECT * FROM stopword"
    return fetchDataFromTable(query, conn)

def get_importantwords(conn):
    query = "SELECT * FROM important_word"
    return fetchDataFromTable(query, conn)

def set_livechatmessage(conn, values):
    return values.to_sql(name = 'livechatmessages', con = conn, if_exists = 'append', index = False)

def set_stopword(conn, values):
    return values.to_sql(name = 'stopword', con = conn, if_exists = 'append', index = False)

def set_importantword(conn, values):
    return values.to_sql(name='important_word', con=conn, if_exists='append', index = False)




####################################################
##################  TASK BLOCKS  ###################
####################################################

def getDataLivechatMessage(**kwargs):
    task_instance   = kwargs['task_instance']
    result = get_livechatmessage(connWarehouseWhmcs)
    task_instance.xcom_push(key = 'dataLiveChat', value = result)
    return result

def getDataStopWord(**kwargs):
    task_instance = kwargs['task_instance']
    result = get_stopword(connWarehouseWhmcs)
    task_instance.xcom_push(key = 'stopWord', value = result)
    return result

def getDataImportantWord(**kwargs):
    task_instance = kwargs['task_instance']
    result = get_importantwords(connWarehouseWhmcs)
    task_instance.xcom_push(key = 'importantWord', value = result)
    return result

def insertLivechatMessages(**kwargs):
    task_instance = kwargs['task_instance']
    data = task_instance.xcom_pull(task_ids='getLiveChat_Message', key= 'dataLiveChat')
    return set_livechatmessage(connWarehouseTestdb, data)

def insertStopWord(**kwargs):
    task_instance = kwargs['task_instance']
    data = task_instance.xcom_pull(task_ids='getStop_Word', key= 'stopWord')
    return set_stopword(connWarehouseTestdb, data)

def insertImporantWord(**kwargs):
    task_instance = kwargs['task_instance']
    data = task_instance.xcom_pull(task_ids='getImportant_Word', key= 'importantWord')
    return set_importantword(connWarehouseTestdb, data)

def startProcess():
    print('START')



####################################################
#############  DEPENDENCIES BLOCKS  ################
####################################################

taskStart = PythonOperator(
    task_id= 'Test_startProcess',
    python_callable=startProcess,
    dag=dag)

taskGetLiveChatMessage = PythonOperator(
    task_id= 'getLiveChat_Message',
    provide_context = True,
    python_callable=getDataLivechatMessage,
    dag=dag)

taskGetStopWord = PythonOperator(
    task_id= 'getStop_Word',
    provide_context = True,
    python_callable=getDataStopWord,
    dag=dag)

taskGetImportantWord = PythonOperator(
    task_id= 'getImportant_Word',
    provide_context = True,
    python_callable=getDataImportantWord,
    dag=dag)

taskInsertLiveChatMessage = PythonOperator(
    task_id= 'setLiveChat_Message',
    provide_context = True,
    python_callable=insertLivechatMessages,
    dag=dag)

taskInsertStopWord = PythonOperator(
    task_id= 'setStop_Word',
    provide_context = True,
    python_callable=insertStopWord,
    dag=dag)

taskInsertImportantWord = PythonOperator(
    task_id= 'setImportant_Word',
    provide_context = True,
    python_callable=insertImporantWord,
    dag=dag)


taskStart >> [taskGetLiveChatMessage, taskGetStopWord, taskGetImportantWord]
taskGetLiveChatMessage >> taskInsertLiveChatMessage
taskGetStopWord >> taskInsertStopWord
taskGetImportantWord >> taskInsertImportantWord