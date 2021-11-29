from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pymysql ## Koneksi ke mysql
from datetime import timedelta, date, datetime
import pandas as pd
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
import re
from collections import Counter

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Important Word Selection',
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
    'Important_Word-Mysql-v1.0-Test',
    default_args=default_args,
    description='A selection process important Word',
    schedule_interval= None,
)


ConfWarehouseDB = BaseHook.get_connection("Warehouse-test") ## Koneksi ke data warehouse

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


####################################################
##################  QUERY BLOCKS  ##################
####################################################


def get_livechatmessage(start, stop, conn):
    query = "SELECT `id`, `loglivechatid`, `text`, `user_type`, `date` FROM `livechatmessages` WHERE DATE(`date`) BETWEEN DATE('" + start + "') AND DATE('" + stop + "')"
    return fetchDataFromTable(query, conn)


def get_stopword(conn):
    query = "SELECT * FROM stopword"
    return fetchDataFromTable(query, conn)


def get_importantwords(conn):
    query = "SELECT word FROM important_word"
    return fetchDataFromTable(query, conn)


def insert_newimportantword(conn, values_insert):
    query = "INSERT INTO important_word (word, main_word, is_usage) VALUES " + values_insert
    return getConnection(query, conn)


####################################################
##############  TASK PROCESS BLOCKS  ###############
####################################################


### process steming dataLiveChat (message) to words.
def stem_LiveChatMessage(**kwargs):
    task_instance   = kwargs['task_instance']
    data = task_instance.xcom_pull(task_ids = 'getLiveChat_Message' , key = 'dataLiveChat')
    data = data.values.tolist()

    words = [[] for _ in range(len(data))]
    data2 = [[] for _ in range(len(data))]
    factory = StemmerFactory()
    stemmer = factory.create_stemmer()

    for index in range(len(data)):
        words[index] = str(re.findall(r"\b[a-zA-Z]{1}\w+", str(data[index][2])))    ## cast every list to string type
            ### the problem is, if the list has non ascii character it sometime defect
        words[index] = stemmer.stem(words[index])        ### stem bahasa
        words[index] = words[index].split()             ### split every bit word in message chat
        data2[index].append(data[index][1])
        data2[index].append(words[index])

    words = pd.DataFrame(words)
    task_instance.xcom_push(key = 'wordsLiveChat', value = words)



def split_Words(**kwargs):
    task_instance = kwargs['task_instance']
    words = task_instance.xcom_pull(task_ids = 'stemLive_Chat', key = 'wordsLiveChat')
    words = words.values.tolist()

    stoplist = task_instance.xcom_pull(task_ids = 'getStop_Word', key = 'stopWord')
    stoplist = [x[0] for x in stoplist.values.tolist()]

    for n in range(len(words)):
        i = [word for word in words[n] if word not in stoplist]
        temp = Counter(i)
        words[n] = [x for x in temp if temp[x] >= 3]  ###

    words = [i for i in words if i != []]

    words_split = []
    for i in words:
        for y in i:
            words_split.append(y)

    words_split = list(dict.fromkeys(words_split))
    words_split = [[x] for x in words_split]

    for i in range(len(words_split)):
        words_split[i].append('')
        words_split[i].append('')

    words_split = pd.DataFrame(words_split)
    task_instance.xcom_push(key = 'splitWord', value = words_split)
    return words_split


def check_StoredImportantWord(**kwargs):
    task_instance = kwargs['task_instance']
    stored_word = task_instance.xcom_pull(task_ids = 'getImportant_Word', key = 'importantWord')
    stored_word = stored_word.values.tolist()

    words_split = task_instance.xcom_pull(task_ids = 'split_Words', key = 'splitWord')
    words_split = words_split.values.tolist()

    words_split.pop(0)

    word_exist = [x[0] for x in stored_word]

    data_insert = []

    for x in words_split:
        if x[0].lower() not in word_exist:
            data_insert.append(x)
        else:
            pass

    values_insert = ''
    if data_insert:
        values_insert = values_insert + str(tuple(data_insert[0]))
        for index in range(1, len(data_insert)):
            values_insert = values_insert + ', ' + str(tuple(data_insert[index]))

    ### push variable string
    task_instance.xcom_push(key = 'insertValue', value = values_insert)
    return values_insert

####################################################
##################  TASK BLOCKS  ###################
####################################################

def getDataLivechatMessage(**kwargs):
    task_instance   = kwargs['task_instance']
    ### date dibawah harus di edit menjadi dr awal bulan sampai akhir bulan
    today = "2019-06-30"
    startdate = "2019-06-01"
    result = get_livechatmessage(startdate, today, connWarehouseWhmcs)
    result = pd.DataFrame(result)
    task_instance.xcom_push(key = 'dataLiveChat', value = result)
    return result


def getDataStopWord(**kwargs):
    task_instance = kwargs['task_instance']
    result = get_stopword(connWarehouseWhmcs)
    result = pd.DataFrame(result)
    task_instance.xcom_push(key = 'stopWord', value = result)
    return result


def getWordImportantWord(**kwargs):
    task_instance = kwargs['task_instance']
    result = get_importantwords(connWarehouseWhmcs)
    result = pd.DataFrame(result)
    task_instance.xcom_push(key = 'importantWord', value = result)
    return result


def insertNewImportantWord(**kwargs):
    task_instance = kwargs['task_instance']
    values_insert = task_instance.xcom_pull(task_ids = 'check_StoredImportantWord', key = 'insertValue')

    conn = connWarehouseWhmcs

    if values_insert :
        insert_newimportantword(conn, values_insert)
    else :
        pass
        print('There is no values to insert. None new data important word')

def startProcess():
    print('START')


def finishProcess():
    print('FINISH')



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
    python_callable=getWordImportantWord,
    dag=dag)

taskStemLiveChat = PythonOperator(
    task_id = 'stemLive_Chat',
    provide_context = True,
    python_callable = stem_LiveChatMessage,
    dag = dag
)

taskSplitWords = PythonOperator(
    task_id = 'split_Words',
    provide_context = True,
    python_callable = split_Words,
    dag = dag
)

taskCheckStoredImportantWord = PythonOperator(
    task_id = 'check_StoredImportantWord',
    provide_context = True,
    python_callable = check_StoredImportantWord,
    dag = dag
)

taskInsertNewImportantWord = PythonOperator(
    task_id = 'insert_NewImportantWord',
    provide_context = True,
    python_callable = insertNewImportantWord,
    dag = dag
)


taskStart >> [taskGetLiveChatMessage, taskGetStopWord, taskGetImportantWord]
taskGetLiveChatMessage >> taskStemLiveChat
[taskGetStopWord, taskStemLiveChat] >> taskSplitWords
[taskGetImportantWord, taskSplitWords] >> taskCheckStoredImportantWord >> taskInsertNewImportantWord

