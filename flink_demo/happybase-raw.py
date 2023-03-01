from confluent_kafka import Consumer, KafkaError, KafkaException
import sys  
import socket
import happybase
import json

# TODO: test mode change later and for other tables
local_conf = {
    'bootstrap.servers':"localhost:9092",
    'client.id':socket.gethostname() + "_consumer_raw",
    'group.id':'happybase_raw'
}

consumer = Consumer(local_conf)
connection = happybase.Connection('localhost', 9090)


# connect to all raw tables
tables = {
        "TH1" : connection.table('TH1_raw'),
        "TH2" : connection.table('TH2_raw'),
        "HVAC1" : connection.table('HVAC1_raw'),
        "HVAC2" : connection.table('HVAC2_raw'),
        "MiAC1" : connection.table('MiAC1_raw'),
        "MiAC2" : connection.table('MiAC2_raw'),
        "Etot" : connection.table('Etot_raw'),
        "MOV1" : connection.table('MOV1_raw'),
        "W1" : connection.table('W1_raw'),
        "Wtot" : connection.table('Wtot_raw'),
}

#TODO:(?)implement a way to safely stop the consumer (finally scope) 
def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)


        counter = 0
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
                global table
                temp_json = json.loads(msg.value())
                
                tables[temp_json['m_name']].put(f'{temp_json["m_timestamp"]}', {b'cf:name': temp_json['m_name'],
                                b'cf:datetime': str(temp_json['m_timestamp']),
                                b'cf:value' : str(temp_json['m_value'])})

                counter += 1

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

basic_consume_loop(consumer, ['output_raw'])
