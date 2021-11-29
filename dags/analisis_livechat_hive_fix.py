# module untuk connection to HiveServer2
from pyhive import hive
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
import re
import pandas as pd
import nltk
import json

# Connection property
host_name = "192.168.169.13"
port = 10000
user = "hive"
password = "admin"
database = "livechat"

# masukkan tanggal awal dan akhir data yang akan diambil dari table livechatmessages
start = '2019-05-01'
stop = '2019-05-03'

# untuk koneksi ke HiveServer2 database livechat
conn = hive.Connection(host=host_name, port=port, username=user, password=password,
                       database=database, auth='CUSTOM')


# function ambil data ke table livechatmessages
def fetch_livechatmessages(start, stop):
    # query edit manual tenggat waktu pengambilan data
    query = "SELECT `id`, `loglivechatid`, `text`, `user_type`, `date` FROM `livechatmessages` WHERE DATE(`date`) BETWEEN DATE('" + start + "') AND DATE('" + stop + "')"
    result = pd.read_sql_query(query, conn).values.tolist()

    return result


def fetch_importantword():
    query = "SELECT word FROM important_word w WHERE is_usage = '1'"
    result = pd.read_sql_query(query, conn).values.tolist()

    return result


# call function fetch_livechatmessages dan important_word
data = fetch_livechatmessages(start, stop)
listwords = fetch_importantword()




##################################################################################################
### Proses Stemming Data
words = [[] for _ in range(len(data))]  # chat # membuat banyak [n] sebanyak len(data)
chat_id = [[] for _ in range(len(words))]  # id-chat
factory = StemmerFactory()
stemmer = factory.create_stemmer()

for index in range(len(data)):
    try:
        words[index] = str(re.findall(r"\b[a-zA-Z]{1}\w+", str(data[index][2])))
        words[index] = stemmer.stem(words[index])
        words[index] = words[index].split()
        chat_id[index].append(data[index][1])
        chat_id[index].append(words[index])
    except:
        pass  # passing setiap teks yang berisi karakter diluar ascii



##################################################################################################
# proses filter kata penting
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





##################################################################################################
### Mengubah data bigram ke bentuk dataframe

df = pd.DataFrame(bigram)
df.columns = ['id', 'loglivechatid', 'tipe', 'percakapan', 'date']
df = df[df.astype(str)['percakapan'] != '[]']
df['percakapan'] = df['percakapan'].apply(json.dumps)
df['percakapan_modify'] = ""

### Proses Sinonim

new_data = df.values.tolist()

list_main = pd.read_sql_query("SELECT * FROM important_word WHERE main_word IS NOT NULL", conn).values.tolist()

p_modify = []
for i in range(len(new_data)):
    p_modify.append(eval(new_data[i][3]))

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





##################################################################################################
def update():
    ### check hasil update
    if new_data :
        ## bikin temp table versi hive

        query = "CREATE TABLE IF NOT EXISTS livechat.temp_table (id BIGINT, loglivechatid STRING, tipe STRING, percakapan STRING, `date` TIMESTAMP, percakapan_modify STRING) " \
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC"
        cur = conn.cursor()
        cur.execute(query)

        values_update = ''
        values_update = values_update + str(tuple(new_data[0]))

        for index in range(1, len(new_data)):
            values_update = values_update + ', ' + str(tuple(new_data[index]))

        query_insert = "INSERT INTO temp_table (id, loglivechatid, tipe, percakapan, `date`, percakapan_modify) values " + values_update
        cur = conn.cursor()
        cur.execute(query_insert)

        ### UPDATE AND INSERT

        query_merge = "MERGE into bigram_livechat trg " \
                      "USING temp_table src " \
                      "ON src.id = trg.id " \
                      "WHEN MATCHED THEN UPDATE SET id = src.id, loglivechatid = src.loglivechatid, tipe = src.tipe, " \
                      "percakapan = src.percakapan, `date` = src.`date`, percakapan_modify = src.percakapan_modify " \
                      "WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.loglivechatid, src.tipe, src.percakapan, " \
                      "src.`date`, src.percakapan_modify)"

        cur = conn.cursor()
        cur.execute(query_merge)

        query = "DROP TABLE temp_table"
        cur = conn.cursor()
        cur.execute(query)

    else :
        print("Tidak Ada Data baru yang ditemukan")


update()
print('succes')
