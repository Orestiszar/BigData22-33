# get data from HBase
import happybase
from datetime import datetime



# connect to HBase's thrift api
def connect_to_database():
    HBASE_SERVER = 'localhost'
    HBASE_PORT = 9090
    conn = happybase.Connection(HBASE_SERVER, HBASE_PORT)
    return conn


# gets raw and aggr data
def get_data(table_name):
    conn = connect_to_database()

    data = []
    try:
        table = conn.table(table_name)
        
        for key, val in table.scan():
            # CHANGE LATER USING mNAME AS TIME
            key = key.decode('utf-8').replace('cf:','')
            value = val[b'cf:value'].decode('utf-8')
            time = val[b'cf:datetime'].decode('utf-8')

            data.append(
                {
                "time":time,
                "value":value
                })
    except Exception as e: 
        print(e)
        data = [{}]

    return(data)
