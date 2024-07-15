from json import dumps
from confluent_kafka import Producer


topic_name = "new_input_documents"
producer = Producer({"bootstrap.servers": "localhost:9092"})

message_dict = {"url": "10", "pub_date": 100, "fetch_time": 400, "text": "300"}

for i in range(5000):
    message_dict["url"] = str(int(message_dict["url"]) + 1)
    msg = dumps(message_dict)
    producer.produce(topic_name, value=msg.encode("utf-8"))
    producer.flush()
