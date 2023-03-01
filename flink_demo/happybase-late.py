from confluent_kafka import Consumer, KafkaError, KafkaException
import sys  
import socket
import happybase
import json

from datetime import datetime

local_conf = {
    'bootstrap.servers':"localhost:9092",
    'client.id':socket.gethostname() + "_consumer_late",
    'group.id':'happybase_late'
}

consumer = Consumer(local_conf)
connection = happybase.Connection('localhost', 9090, timeout=100000)

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
                temp_json = json.loads(msg.value())

                if(temp_json['m_name'] == "W1_toprocess"):
                    table = connection.table('late_processed')
                else:
                    table = connection.table('late_rejected')

                table.put(f'{temp_json["m_timestamp"]}', {b'cf:name': 'W1',
                                b'cf:datetime': str(temp_json['m_timestamp']),
                                b'cf:value' : str(temp_json['m_value'])})
                counter += 1
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

basic_consume_loop(consumer, ['input_late'])

