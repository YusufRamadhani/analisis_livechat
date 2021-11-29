# module untuk connection to HiveServer2
from pyhive import hive
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
import pandas as pd
import re
from collections import Counter

# Connection property
host_name = "hive-dwh"
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

    query = "SELECT `id`, `loglivechatid`, `text`, `user_type`, `date` FROM `livechatmessages` WHERE DATE(`date`) BETWEEN DATE('" + \
        start + "') AND DATE('" + stop + "')"
    result = pd.read_sql_query(query, conn).values.tolist()
    return result


data = fetch_livechatmessages(start, stop)

words = [[] for _ in range(len(data))]
data2 = [[] for _ in range(len(data))]
factory = StemmerFactory()
stemmer = factory.create_stemmer()

for index in range(len(data)):
    try:
        words[index] = str(re.findall(
            r"\b[a-zA-Z]{1}\w+", str(data[index][2])))
        words[index] = stemmer.stem(words[index])
        words[index] = words[index].split()
        data2[index].append(data[index][1])
        data2[index].append(words[index])
    except:
        pass  # passing setiap teks yang berisi karakter diluar ascii

sw = pd.read_sql_query("SELECT * FROM stopword ", conn)


#######################################################
# list berisi stopwords
# stoplist is list [0] in sw value
stoplist = [x[0] for x in sw.values.tolist()]

# data2 berisi [[loglovechatid]['words','words'.....'words']]
# check jika kata yang baru di stem tidak ada di stoplist
for i in data2:
    i[1] = [word for word in i[1] if word not in stoplist]  # apakah ini digunakan?


for n in range(len(words)):
    i = [word for word in words[n] if word not in stoplist]
    temp = Counter(i)
    words[n] = [x for x in temp if temp[x] >= 3]


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
############################################################


def insert_important_word():

    # cek important word yg sudah ada

    stored_word = pd.read_sql_query(
        "SELECT word FROM important_word", conn).values.tolist()
    ##################################################################
    word_exist = [x[0] for x in stored_word]

    data_insert = []

    for x in words_split:
        if x[0].lower() not in word_exist:
            data_insert.append(x)
        else:
            pass

    if data_insert:
        values_insert = ''
        values_insert = values_insert + str(tuple(data_insert[0]))
        for index in range(1, len(data_insert)):
            values_insert = values_insert + ', ' + \
                str(tuple(data_insert[index]))
    ############################################################################
        query_insert = "INSERT INTO important_word (word, main_word, is_usage) VALUES " + \
            values_insert

        print("isi dari query insert " + query_insert)
        cur = conn.cursor()
        cur.execute(query_insert)
    else:
        print("tidak ada word_important yang baru")


insert_important_word()
print("succes")
