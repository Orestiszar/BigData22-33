import random
import datetime
import time
import numpy as np

def create_data_15min():
    TH1=np.random.uniform(12,35)
    TH2=np.random.uniform(12,35)
    HVAC1=np.random.uniform(0,100)
    HVAC2=np.random.uniform(0,200)
    MiAC1=np.random.uniform(0,150)
    MiAC2=np.random.uniform(0,200)
    W1=np.random.uniform(0,1)
    return (TH1,TH2,HVAC1,HVAC2,MiAC1,MiAC2,W1)
    
def create_data_1day():
    global wtotenergy, etotenergy
    etot = np.random.uniform(-1000,1000)
    etotenergy += (2600*24 + etot)
    wtot = np.random.uniform(-10,10)
    wtotenergy += (100+wtot)

def MOV1_old():
    return np.random.uniform(0,1)

moving_list = [1 if i < 5 else 0 for i in range(96)]
random.shuffle(moving_list)

date=datetime.datetime(2023,1,1)
etotenergy=0
wtotenergy=0
secondcounter=0

while(1):
    if(moving_list[secondcounter%96]==1):
        tempdate = date + datetime.timedelta(minutes=random.randint(0,15),seconds=random.randint(0,60))
        print(tempdate,"1")
    if(secondcounter%96 == 0 and secondcounter!=0):
        create_data_1day()
        print(etotenergy, wtotenergy, "Called")
        random.shuffle(moving_list)
    if(secondcounter%20==0 and secondcounter!=0):
        tempdate_mov = date-datetime.timedelta(days=2)
        print(MOV1_old(),tempdate_mov)
    if(secondcounter%120==0 and secondcounter!=0):
        tempdate_mov = date-datetime.timedelta(days=10)
        print(MOV1_old(),tempdate_mov)
    # (TH1,TH2,HVAC1,HVAC2,MiAC1,MiAC2,W1) = create_data_15min()
    print(create_data_15min())
    date+=datetime.timedelta(minutes=15)
    # print(date)
    #feed ston kafka
    secondcounter+=1
    time.sleep(1)