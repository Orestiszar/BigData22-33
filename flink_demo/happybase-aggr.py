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

                connection = happybase.Connection('localhost', 9090)

                table = connection.table(temp_json['m_name'] + '_aggr')

                try:
                    table.put(f'{temp_json["m_name"]}_{counter}', {b'cf:name': temp_json['m_name'],
                                    b'cf:datetime': str(temp_json['the_timestamp']),
                                    b'cf:value' : str(temp_json['window_daily_values'])})
                except:
                    print(f'EXCEPTION IN : Counter: {counter} || Value: {str(msg.value())}')

                counter += 1

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

basic_consume_loop(consumer, ['output_aggr'])
