import threading
import logging
import time
import json
import multiprocessing
from kafka import KafkaProducer, KafkaConsumer
from PostegreSQL_impl import create_tables, insert_data


class Producer(threading.Thread):                                       # Our Kafka Producer using threading
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        producer = KafkaProducer(bootstrap_servers="kafka-241a0629-konstatheodoridi-30f9.aivencloud.com:27477",
                                 security_protocol='SSL',
                                 ssl_cafile="ca.pem",
                                 ssl_certfile="service.cert",
                                 ssl_keyfile="service.key",             # configuration details and serializer as
                                 retries=5,                             # I'm using Json messages in my example
                                 client_id="client_0",
                                 value_serializer=lambda l: json.dumps(l).encode('utf-8'))
        producer.send('Demo-topic', {'John Johnson': '2000'}).get(3000)
        producer.send('Demo-topic', {'Mike Smith': '3000'}).get(3000)
        producer.send('Demo-topic', {'Kostas Theodoridis': '10'}).get(3000)
        time.sleep(5)
        producer.flush()
        producer.close()


class Consumer(multiprocessing.Process):                                # Our Kafka Consumer using multiprocessing
    def __init__(self, group_id, client_id):
        multiprocessing.Process.__init__(self)
        self.group_id = group_id
        self.client_id = client_id

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='kafka-241a0629-konstatheodoridi-30f9.aivencloud.com:27477',   # configuration details and we set a group for
                                 security_protocol='SSL',
                                 ssl_cafile="ca.pem",
                                 ssl_certfile="service.cert",
                                 ssl_keyfile="service.key",
                                 auto_offset_reset='earliest',          # parallel use of multiple consumers
                                 consumer_timeout_ms=1000,
                                 group_id='group_id', client_id='client_id',
                                 value_deserializer=lambda l: json.loads(l.decode('utf-8')))
        consumer.subscribe(['Demo-topic'])                              # subscribe to a list of topics

        while True:
            for message in consumer:
                print("Details:{}".format(message))
                print("Printed from consumer with client id: {0} "
                      "and group id: {1}".format(self.client_id, self.group_id))
                #for key, value in message.value.items():
                    # print("Key {0} and value {1}".format(key, value))
                    # insert_data(key, int(value))


def main():
    tasks = [
        Consumer('group1', 'client1'),                           # Creation of two consumers in the same group
        Consumer('group1', 'client2'),                           # to test using them in parallel
        Producer()
    ]
    for task in tasks:
        task.start()
    time.sleep(10)
    create_tables()                                             # create table in the PostgrSQL database
    for task in tasks:
        task.join()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
