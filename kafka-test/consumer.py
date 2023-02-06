from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import socket
from kafka_config import conf

conf['client.id'] = socket.gethostname() + "_consumer"
conf['group.id'] = "test"
conf['auto.offset.reset'] = 'smallest'

consumer = Consumer(conf)

#TODO:(?)implement a way to safely stop the consumer (finally scope) 
def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

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
                print(f'Key: {str(msg.key())}, Value: {str(msg.value())}')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

basic_consume_loop(consumer, ['test_topic'])