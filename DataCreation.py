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

moving_list = [1 if i < 5 else 0 for i in range(96)]
random.shuffle(moving_list)

date=datetime.datetime(2023,1,1)
etotenergy=0
wtotenergy=0
secondcounter=0

# print(date, date.strftime("%YY/%MM %H:%M:%S"))
while(1):
    if(secondcounter%96 == 0 and secondcounter!=0):
        create_data_1day()
        print(etotenergy, wtotenergy, "Called")
    (TH1,TH2,HVAC1,HVAC2,MiAC1,MiAC2,W1) = create_data_15min()
    date+=datetime.timedelta(minutes=15)
    # print(date)
    #feed ston kafka
    secondcounter+=1
    time.sleep(1)