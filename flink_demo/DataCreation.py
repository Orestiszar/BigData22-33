import random
import datetime
from sqlite3 import Timestamp
import time
import numpy as np
import json

# kafka imports and config
from confluent_kafka import Producer
import socket

# used for acknowledgment if the msg was produced succesfully 
def kafka_callback(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))

def kafka_produce(data_dict, timestamp):
    global key
    for name, value in data_dict.items():
        json_temp = {
            "m_name" : name, 
            "m_value" : value,
            "m_timestamp" : str(timestamp)
        }
        # cast to json
        msg = json.dumps(json_temp)
    
        producer.produce('input', key=str(key), value=msg, callback=kafka_callback)
        # TODO: check with callback if the message was really delivered
        producer.poll(2)
        key += 1


def create_data_15min():
    data_dict = {}
    
    data_dict['TH1']=np.random.uniform(12,35)
    data_dict['TH2']=np.random.uniform(12,35)
    
    data_dict['HVAC1']=np.random.uniform(0,100)
    data_dict['HVAC2']=np.random.uniform(0,200)

    data_dict['MiAC1']=np.random.uniform(0,150)
    data_dict['MiAC2']=np.random.uniform(0,200)
    
    data_dict['W1']=np.random.uniform(0,1)
    
    return data_dict
    

def create_data_1day():
    global wtotenergy, etotenergy
    
    etot = np.random.uniform(-1000,1000)
    etotenergy += (2600*24 + etot)
    
    wtot = np.random.uniform(-10,10)
    wtotenergy += (100+wtot)


#TODO: remove(?)
def MOV1_old():
    return np.random.uniform(0,1)

def create_data():
    # used for picking random samples during the day(?)
    moving_list = [1 if i < 5 else 0 for i in range(96)]
    random.shuffle(moving_list)

    # init metric values and date
    date=datetime.datetime(2023,1,1)
    etotenergy=0
    wtotenergy=0
    secondcounter=0

    # create the producer used for kafka
    conf_local = {
        'bootstrap.servers':"localhost:9092",
        'client.id' : socket.gethostname() + "_producer"
    }

    producer = Producer(conf_local)

    # key counter for kafka (used just in case)
    key = 0

    while(1):
        # produce if it is time for MOV1
        if(moving_list[secondcounter%96]==1):
            tempdate = date + datetime.timedelta(minutes=random.randint(0,15),seconds=random.randint(0,60))
            
            kafka_produce({'MOV1' : 1}, tempdate)

        # produce required data at the end of each day
        if(secondcounter%96 == 0 and secondcounter != 0):
            create_data_1day()
            
            random.shuffle(moving_list)
            kafka_produce({'Etot' : etotenergy, 'Wtot': wtotenergy}, date)
            
            
        # if(secondcounter%20==0 and secondcounter!=0):
        #     tempdate_mov = date-datetime.timedelta(days=2)
        #     print(MOV1_old(),tempdate_mov)

        # if(secondcounter%120==0 and secondcounter!=0):
        #     tempdate_mov = date-datetime.timedelta(days=10)
        #     print(MOV1_old(),tempdate_mov)

        # produce data for each 15 min
        kafka_produce(create_data_15min(), date)
        date += datetime.timedelta(minutes=15)
        
        secondcounter+=1
        time.sleep(1)