import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pymysql ## Koneksi ke mysql
from datetime import timedelta, date, datetime
import pandas as pd
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
import re
import nltk
import json


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Analisis Live Chat',
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
    'Analisis_Livechat-Mysql-v1.0-Test',
    default_args=default_args,
    description='A process mining text from livechat',
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



########################################
##### Query job block #####
########################################


### query to load table livechatmessages. the input for stemDataLive_Chat process
def get_livechatmessage(start, stop, conn):
    query = "SELECT id, loglivechatid, text, user_type, `date` FROM livechatmessages WHERE DATE(`date`) BETWEEN DATE('" + start + "') AND DATE('" + stop + "')"
    return fetchDataFromTable(query, conn)


### query to load table important_word. the data named listwords and it used for bigram_Words process
def get_importantword(conn):
    query = "SELECT word FROM important_word w WHERE is_usage = '1'"
    return fetchDataFromTable(query, conn)


### query to load data, data named list_main used for synonymMain_Word
def get_importantword_main(conn):
    query = "SELECT * FROM important_word WHERE main_word IS NOT NULL"
    return fetchDataFromTable(query, conn)


### update for matched row and insert for new data (update process)
def merge_tables(values, conn):
    query = "INSERT IGNORE INTO bigram_livechat (id, loglivechatid, tipe, percakapan, `date`, percakapan_modify) VALUES " + values
    return getConnection(query, conn)


########################################
##### Process block #####
########################################


### process steming dataLiveChat (message) to words. next process is bigramWords
def stemDataLiveChat(**kwargs):
    task_instance   = kwargs['task_instance']
    data = task_instance.xcom_pull(task_ids='getLiveChat_Message', key='dataLiveChat')
    data = data.values.tolist()

    words = [[] for _ in range(len(data))]  # chat
    chat_id = [[] for _ in range(len(words))]  # id-chat
    factory = StemmerFactory()
    stemmer = factory.create_stemmer()

    for index in range(len(data)):
        # try:
            words[index] = str(re.findall(r"\b[a-zA-Z]{1}\w+", str(data[index][2]))) ## cast every list to string type
            ### the problem is, if the list has non ascii character it sometime defect
            words[index] = stemmer.stem(words[index])   ### stem bahasa
            words[index] = words[index].split()         ### split every bit word in message chat
            chat_id[index].append(data[index][1])
            chat_id[index].append(words[index])
        # except:
        #     pass  # passing setiap teks yang berisi karakter diluar ascii

    words = pd.DataFrame(words)
    task_instance.xcom_push(key='stemWords', value=words)
    return words


### process bigram used words and dataLiveChat and the output is bigram words
def bigramWords(**kwargs):
    task_instance   = kwargs['task_instance']
    words = task_instance.xcom_pull(task_ids = 'stemDataLive_Chat', key = 'stemWords')
    words = words.values.tolist()
    data = task_instance.xcom_pull(task_ids = 'getLiveChat_Message', key = 'dataLiveChat')
    data = data.values.tolist()
    listwords = task_instance.xcom_pull(task_ids = 'getImportant_Word', key = 'listImportantWord')
    listwords = listwords.values.tolist()


    ##### the process mining text isnt use stopword. instead the filter every word is use important_words
    ##### which is important word has selected before by another text mining process
    filtered = [[] for _ in range(len(words))]

    for i in range(len(words)):
        for j in range(len(words[i])):
            try:
                if re.search(r'\b' + words[i][j] + r'\b', str(listwords)):
                    filtered[i].append(words[i][j])
            except:
                print(i, j, words[i][j])

    bigram = [[] for _ in range(len(filtered))]

    for i in range(len(filtered)):
        bigram[i].append(data[i][0])
        bigram[i].append(data[i][1])
        bigram[i].append(data[i][3])
        if (len(filtered[i]) > 1):
            bigram[i].append(list(nltk.bigrams(filtered[i])))
        else:
            bigram[i].append(filtered[i])

    # grouping by id
    loglivechatid = list(set(map(lambda x: x[1], bigram)))
    groupdata = [[y[3] for y in bigram if y[1] == x] for x in loglivechatid]

    # cleaning empty list
    bigramid = [[] for _ in range(len(groupdata))]

    for i in range(len(groupdata)):
        bigramid[i] = list(filter(None, groupdata[i]))

    # pairing
    datadict = {}

    for index in range(len(loglivechatid)):
        datadict.update({loglivechatid[index]: groupdata[index]})

    for x in data:
        if not x[4]:
            print(x)

    for i in range(len(data)):
        if data[i][4]:
            bigram[i].append(data[i][4])

    bigram = pd.DataFrame(bigram)
    task_instance.xcom_push(key = 'bigramWords', value = bigram)
    return bigram


### add column in data frame
def addColumn(df):
    df.columns = ['id', 'loglivechatid', 'tipe', 'percakapan', 'date']
    df = df[df.astype(str)['percakapan'] != '[]']
    df['percakapan'] = df['percakapan'].apply(json.dumps)
    df['percakapan_modify'] = ""
    return df


### Checking synonym new data (the data is birgam format) for match with recorded  main word from table important_word
def synonymMainWord(**kwargs):
    task_instance = kwargs['task_instance']
    list_main = task_instance.xcom_pull(task_ids = 'getImportantMain_Word', key = 'listMainImportantWord')
    list_main = list_main.values.tolist()
    df = task_instance.xcom_pull(task_ids = 'bigram_Words', key = 'bigramWords')
    df = addColumn(df)

    new_data = df.values.tolist()

    p_modify = []
    for i in range(len(new_data)):
        p_modify.append(eval(new_data[i][3]))


    ### matching the new data (bigram) with the main word. the 'main word' is the 'important word' which selected by person
    ### that was the real important for costumer care dept
    for i in range(len(p_modify)):
        for j in range(len(p_modify[i])):
            if type(p_modify[i][j]) == str:
                for k in list_main:
                    if p_modify[i][j] == k[1]:
                        p_modify[i][j] = k[2]
                        break
            else:
                for z in range(len(p_modify[i][j])):
                    for k in list_main:
                        if p_modify[i][j][z] == k[1]:
                            p_modify[i][j][z] = k[2]
                            break

    for i in range(len(new_data)):
        new_data[i][5] = str(p_modify[i])

    new_dataLiveChat = pd.DataFrame(new_data)
    task_instance.xcom_push(key = 'newDataLiveChat' , value = new_dataLiveChat)
    return new_dataLiveChat


########################################
##### Task block #####
########################################


### Store dataLiveChat to Xcom var
def getDataLivechatMessage(**kwargs):
    task_instance   = kwargs['task_instance']
    ### date dibawah harus di edit menjadi dr awal bulan sampai akhir bulan
    today = "2019-06-30"
    startdate = "2019-06-01"
    result = get_livechatmessage(startdate, today, connWarehouseWhmcs)
    result = pd.DataFrame(result)
    task_instance.xcom_push(key = 'dataLiveChat', value = result)
    return result


### Store listImportantWord to Xcom var
def getImportantWord(**kwargs):
    task_instance   = kwargs['task_instance']
    result = get_importantword(connWarehouseWhmcs)
    result = pd.DataFrame(result)
    task_instance.xcom_push(key='listImportantWord', value=result)
    return result

### Store listMainImportantWord to Xcom var
def getMainImportantWord(**kwargs):
    task_instance   = kwargs['task_instance']
    result = get_importantword_main(connWarehouseWhmcs)
    result = pd.DataFrame(result)
    task_instance.xcom_push(key='listMainImportantWord', value=result)
    return result


### the update bigram_livechat table
def storeNewData(**kwargs):
    task_instance   = kwargs['task_instance']
    value = task_instance.xcom_pull(task_ids = 'synonymMain_Word', key = 'newDataLiveChat')
    value = value.values.tolist()
    values_update = ''
    values_update = values_update + str(tuple(value[0]))

    for index in range(1, len(value)):
        values_update = values_update + ', ' + str(tuple(value[index]))

    conn = connWarehouseWhmcs
    merge_tables(values_update, conn)


def startProcess():
    print('START')

def finishProcess():
    print('FINISH')


taskStart = PythonOperator(
    task_id= 'Test_startProcess',
    python_callable=startProcess,
    dag=dag)

taskGetLiveChatMessage = PythonOperator(
    task_id= 'getLiveChat_Message',
    provide_context = True,
    python_callable=getDataLivechatMessage,
    dag=dag)

taskGetImportantWord = PythonOperator(
    task_id = 'getImportant_Word',
    provide_context = True,
    python_callable = getImportantWord,
    dag = dag)

taskStemDataLiveChat = PythonOperator(
    task_id = 'stemDataLive_Chat',
    provide_context = True,
    python_callable = stemDataLiveChat,
    dag = dag
)

taskGetImportantMainWord = PythonOperator(
    task_id = 'getImportantMain_Word',
    provide_context = True,
    python_callable = getMainImportantWord,
    dag = dag
)

taskBigramWords = PythonOperator(
    task_id = 'bigram_Words',
    provide_context = True,
    python_callable = bigramWords,
    dag = dag
)

taskSynonymMainWord = PythonOperator(
    task_id = 'synonymMain_Word',
    provide_context = True,
    python_callable = synonymMainWord,
    dag = dag
)

taskStoreNewData = PythonOperator(
    task_id = 'storeNew_Data',
    provide_context = True,
    python_callable = storeNewData,
    dag = dag
)

taskFinish = PythonOperator(
    task_id= 'Test_finishProcess',
    python_callable=finishProcess,
    dag=dag)


taskStart >> [taskGetLiveChatMessage, taskGetImportantWord, taskGetImportantMainWord]
taskGetLiveChatMessage >> taskStemDataLiveChat
[taskStemDataLiveChat, taskGetImportantWord] >> taskBigramWords
[taskGetImportantMainWord, taskBigramWords] >> taskSynonymMainWord >> taskStoreNewData >> taskFinish