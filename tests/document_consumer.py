import logging

from time import time
from confluent_kafka import Consumer, KafkaError


c = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "my_consumer_group",
        "auto.offset.reset": "earliest",
    }
)


c.subscribe(["output_documents"])

count = 0
while count < 5000:
    if count == 1:
        start = time()
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            logging.error(msg.error())
            break
    logging.info("Received message: {}".format(msg.value().decode("utf-8")))
    count += 1

logging.info(time() - start)
# Close consumer
c.close()
