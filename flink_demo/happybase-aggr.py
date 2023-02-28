from confluent_kafka import Consumer, KafkaError, KafkaException
import sys  
import socket
import happybase
import json

local_conf = {
    'bootstrap.servers':"localhost:9092",
    'client.id':socket.gethostname() + "_consumer_aggr",
    'group.id':'happybase_aggr'
}

consumer = Consumer(local_conf)
connection = happybase.Connection('localhost', 9090)

tables = {
        "TH1" : connection.table('TH1_aggr'),
        "TH2" : connection.table('TH2_aggr'),
        "HVAC1" : connection.table('HVAC1_aggr'),
        "HVAC2" : connection.table('HVAC2_aggr'),
        "MiAC1" : connection.table('MiAC1_aggr'),
        "MiAC2" : connection.table('MiAC2_aggr'),
        "Etot" : connection.table('Etot_aggr'),
        "MOV1" : connection.table('MOV1_aggr'),
        "W1" : connection.table('W1_aggr'),
        "Wtot" : connection.table('Wtot_aggr'),
}

#TODO:(?)implement a way to safely stop the consumer (finally scope) 
def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        batch_size = 0
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

                tables[temp_json['m_name']].put(f'TH1_{counter}', {b'cf:name': temp_json['m_name'],
                                b'cf:datetime': str(temp_json['m_timestamp']),
                                b'cf:value' : str(temp_json['m_value'])})

                counter += 1

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

basic_consume_loop(consumer, ['sales-euros'])
