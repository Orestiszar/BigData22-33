# get data from HBase
import happybase
from datetime import datetime



# connect to HBase's thrift api
def connect_to_database():
    HBASE_SERVER = 'localhost'
    HBASE_PORT = 9090
    conn = happybase.Connection(HBASE_SERVER, HBASE_PORT)
    return conn


def get_dummy_sum():
    conn = connect_to_database()

    data = []
    try:
        table = conn.table('test')
        
        for key, val in table.scan():
            # CHANGE LATER USING mNAME AS TIME
            key = key.decode('utf-8').replace('cf:','')
            value = val[b'cf:window_daily_values'].decode('utf-8')
            time = val[b'cf:m_name'].decode('utf-8')

            data.append(
                {
                "sensor-id":"dummy_data",
                "time":time,
                "value":value
                })
    except Exception as e: 
        print(e)
        data = [{}]

    return(data)