from time import sleep
from confluent_kafka import Producer
import socket
from kafka_config import conf 
conf['client.id'] = socket.gethostname() + "_producer"

producer = Producer(conf)

# callback method that logs if the mesaage failed to be delivered or it was succesful
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


counter = 0
while True:
    producer.produce('test_topic', key=f"{counter}", value=f"value_{counter}", callback=acked)

    counter += 1
    # check every 2 seconds. Will call the callback
    producer.poll(2)
    sleep(5)