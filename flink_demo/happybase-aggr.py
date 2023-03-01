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

                # add to aggr daily diff for Etot
                if(temp_json['m_name'] == 'Etot'):
                    
                    # insert appropriate time for aggregations
                    dt_object = datetime.strptime(str(temp_json["m_timestamp"]), '%Y-%m-%d %H:%M:%S')
                    dt_object = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)
                    dt_string = dt_object.strftime('%Y-%m-%d %H:%M:%S')
                    
                    table = connection.table(temp_json['m_name']+'_DailyDiff')

                    table.put(f'{dt_string}', {b'cf:name': temp_json['m_name']+'_DailyDiff',
                                b'cf:datetime': str(dt_string),
                                b'cf:value' : str(float(temp_json['m_value']) - etot_prev_sum)})
                    etot_prev_sum += float(temp_json['m_value'])
                    continue
                    
                
                # add to aggr daily diff for Wtot
                if(temp_json['m_name'] == 'Wtot'):

                # insert appropriate time for aggregations
                    dt_object = datetime.strptime(str(temp_json["m_timestamp"]), '%Y-%m-%d %H:%M:%S')
                    dt_object = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)
                    dt_string = dt_object.strftime('%Y-%m-%d %H:%M:%S')
                
                    table = connection.table(temp_json['m_name']+'_DailyDiff')

                    table.put(f'{dt_string}', {b'cf:name': temp_json['m_name']+'_DailyDiff',
                                b'cf:datetime': str(dt_string),
                                b'cf:value' : str(float(temp_json['m_value']) - wtot_prev_sum)})
                    wtot_prev_sum += float(temp_json['m_value'])
                    continue

                table = connection.table(temp_json['m_name'] + '_aggr')

                # insert appropriate time for aggregations
                dt_object = datetime.strptime(str(temp_json['the_timestamp']), '%Y-%m-%d %H:%M:%S')
                dt_object = dt_object.replace(hour=0, minute=0, second=0, microsecond=0)
                dt_string = dt_object.strftime('%Y-%m-%d %H:%M:%S')
                
                table.put(f'{dt_string}', {b'cf:name': temp_json['m_name'],
                                b'cf:datetime': str(dt_string),
                                b'cf:value' : str(temp_json['window_daily_values'])})

                

                counter += 1

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

basic_consume_loop(consumer, ['output_aggr'])
