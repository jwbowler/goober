import time
import random
from kafka import SimpleProducer, KafkaClient
import logging
import json
import sys

logging.basicConfig()

kafka = KafkaClient("52.88.49.174")
producer = SimpleProducer(kafka, async=True, batch_send_every_n=10000, async_queue_maxsize=10000)

def send(k=0):
    try:
        producer.send_messages("m2", line)
    except RuntimeError as e:
        print e
        time.sleep(0.5 * (2 ** k))
        send(k+1)

while True:
    counter = 0

    with open('monday-all.csv', 'r') as replay_file:
        for line in replay_file:
            if counter != 0:
                send()
                if counter % 10000 == 0:
                    print counter

            counter += 1
