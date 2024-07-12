from confluent_kafka import Producer
import json


topic_name = 'input_documents'
producer = Producer({'bootstrap.servers': 'localhost:9092'})

message_dict = {
    'url' : "1",
    'pub_date' : 1,
    'fetch_time' : 1,
    'text' : "text"
}

# for i in range(1000):
msg = json.dumps(message_dict)
producer.produce(topic_name, value=msg.encode("utf-8"))
producer.flush()