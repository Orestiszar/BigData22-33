from confluent_kafka import Consumer, KafkaError, KafkaException
import sys  
import socket
import happybase
import json

from datetime import datetime

local_conf = {
    'bootstrap.servers':"localhost:9092",
    'client.id':socket.gethostname() + "_consumer_aggr",
    'group.id':'happybase_aggr'
}

consumer = Consumer(local_conf)


#TODO:(?)implement a way to safely stop the consumer (finally scope) 
def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        counter = 0
        etot_prev_sum = 0
        wtot_prev_sum = 0
        cur_HVAC1 = 0
        cur_HVAC2 = 0
        cur_MIAC1 = 0
        cur_MIAC2 = 0
        cur_W1 = 0
        cur_Etot = 0
        cur_Wtot = 0
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(f'Counter: {counter} || Value: {str(msg.value())}')

                temp_json = json.loads(msg.value())

                connection = happybase.Connection('localhost', 9090)

                # # add to aggr daily diff for Etot
                # if(temp_json['m_name'] == 'Etot'):
                    
                #     # insert appropriate time for aggregations
                #     dt_object = datetime.strptime(str(temp_json["m_timestamp"]), '%Y-%m-%d %H:%M:%S')
                #     dt_object = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)
                #     dt_string = dt_object.strftime('%Y-%m-%d %H:%M:%S')
                    
                #     table = connection.table(temp_json['m_name']+'_DailyDiff')

                #     table.put(f'{dt_string}', {b'cf:name': temp_json['m_name']+'_DailyDiff',
                #                 b'cf:datetime': str(dt_string),
                #                 b'cf:value' : str(float(temp_json['m_value']) - etot_prev_sum)})
                #     etot_prev_sum += float(temp_json['m_value'])
                #     continue
                    
                
                # # add to aggr daily diff for Wtot
                # if(temp_json['m_name'] == 'Wtot'):

                #     # insert appropriate time for aggregations
                #     dt_object = datetime.strptime(str(temp_json["m_timestamp"]), '%Y-%m-%d %H:%M:%S')
                #     dt_object = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)
                #     dt_string = dt_object.strftime('%Y-%m-%d %H:%M:%S')
                
                #     table = connection.table(temp_json['m_name']+'_DailyDiff')

                #     table.put(f'{dt_string}', {b'cf:name': temp_json['m_name']+'_DailyDiff',
                #                 b'cf:datetime': str(dt_string),
                #                 b'cf:value' : str(float(temp_json['m_value']) - wtot_prev_sum)})
                #     wtot_prev_sum += float(temp_json['m_value'])
                #     continue

                # save aggregations for aggdayrest (TH1 TH2 MOV1 MISSING/NOT NEEDED)
                if(temp_json['m_name'] == 'Wtot'):
                    cur_Wtot = float(temp_json['window_daily_values'])

                elif(temp_json['m_name'] == 'W1'):
                    cur_W1 = float(temp_json['window_daily_values'])
                
                elif(temp_json['m_name'] == 'Etot'):
                    cur_Etot = float(temp_json['window_daily_values'])

                elif(temp_json['m_name'] == 'HVAC1'):
                    cur_HVAC1 = float(temp_json['window_daily_values'])

                elif(temp_json['m_name'] == 'HVAC2'):
                    cur_HVAC2 = float(temp_json['window_daily_values'])

                elif(temp_json['m_name'] == 'MiAC1'):
                    cur_MIAC1 = float(temp_json['window_daily_values'])

                elif(temp_json['m_name'] == 'MiAC2'):
                    cur_MIAC2 = float(temp_json['window_daily_values'])


                # insert appropriate time for aggregations
                dt_object = datetime.strptime(str(temp_json["the_timestamp"]), '%Y-%m-%d %H:%M:%S')
                dt_object = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)
                dt_string = dt_object.strftime('%Y-%m-%d %H:%M:%S')

                if(counter % 9):
                    #AggDayDiff[Etot] - AggDay[HVAC1] - AggDay[HVAC2] - AggDay[MiAC1] - AggDay[MiAC2]
                    AggDayRestEtot = cur_Etot - etot_prev_sum - cur_HVAC1 - cur_HVAC2 - cur_MIAC1 - cur_MIAC2
                    
                    table = connection.table('Etot_AggDayRest')
                    table.put(f'{dt_string}', {b'cf:name': 'Etot_AggDayRest',
                                b'cf:datetime': str(dt_string),
                                b'cf:value' : str(AggDayRestEtot)})
                    
                    #AggDayDiff[Wtot] – AggDay[W1]
                    AggDayRestWtot = cur_Wtot - wtot_prev_sum - cur_W1

                    table = connection.table('Wtot_AggDayRest')
                    table.put(f'{dt_string}', {b'cf:name': 'Wtot_AggDayRest',
                                b'cf:datetime': str(dt_string),
                                b'cf:value' : str(AggDayRestWtot)})
                    
                    #AggDayDiff[Etot]
                    table = connection.table('Etot_DailyDiff')

                    table.put(f'{dt_string}', {b'cf:name': 'Etot_DailyDiff',
                                b'cf:datetime': str(dt_string),
                                b'cf:value' : str(cur_Etot - etot_prev_sum)})
                    etot_prev_sum = cur_Etot

                    #AggDayDiff[Wtot]
                    table = connection.table('Wtot_DailyDiff')

                    table.put(f'{dt_string}', {b'cf:name': 'Wtot_DailyDiff',
                                b'cf:datetime': str(dt_string),
                                b'cf:value' : str(cur_Wtot - wtot_prev_sum)})
                    wtot_prev_sum = cur_Wtot

                if(temp_json['m_name']!= 'Wtot' and temp_json['m_name'] != 'Etot'):
                    table = connection.table(temp_json['m_name'] + '_aggr')

                    table.put(f'{dt_string}', {b'cf:name': temp_json['m_name'],
                                    b'cf:datetime': str(dt_string),
                                    b'cf:value' : str(temp_json['window_daily_values'])})

                counter += 1

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

basic_consume_loop(consumer, ['output_aggr'])
