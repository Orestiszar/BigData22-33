from confluent_kafka import Consumer, KafkaError, KafkaException
import sys  
import socket
import happybase
import json

local_conf = {
    'bootstrap.servers':"localhost:9092",
    'client.id':socket.gethostname() + "_consumer",
    'group.id':'test'
}

consumer = Consumer(local_conf)
connection = happybase.Connection('localhost', 9090)
table = connection.table('test')
batch = table.batch(transaction=True)

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

                batch.put(f'example-consumer_{counter}_{batch_size}', {b'cf:m_name': temp_json['m_name'],
                                    b'cf:window_daily_values': str(temp_json['window_end'])})

                batch_size += 1
                if batch_size == 6:
                    batch.send()
                    counter += 1
                    batch_size == 0

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

basic_consume_loop(consumer, ['sales-euros'])
