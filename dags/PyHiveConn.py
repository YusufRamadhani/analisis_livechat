from pyhive import hive

host_name = "hive-dwh"
port = 10000
user = "hive"
password = "admin"
database = "livechat"


def hiveconnection(host_name, port, user, password, database):
    conn = hive.Connection(host=host_name, port=port, username=user, password=password,
                           database=database, auth='CUSTOM')
    cur = conn.cursor()
    cur.execute('select * from tblloglivechat limit 5')
    result = cur.fetchall()

    return result


# Call above function
output = hiveconnection(host_name, port, user, password, database)
print(output)
