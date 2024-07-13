import json
from confluent_kafka import Consumer, KafkaError, Producer
from documents_processor import DocumentsProcessor


class DocumentsHandler:
    def __init__(self) -> None:
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'aaa',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(['new_input_documents'])

        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})

        self.processor = DocumentsProcessor()
    
    def run(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            
            tdocument = json.loads(msg.value().decode('utf-8'))
            print('Received message: {}'.format(msg.value().decode('utf-8')))
            res = self.processor.process_document(url=tdocument['url'],
                                            pub_date=tdocument['pub_date'],
                                            fetch_time=tdocument['fetch_time'],
                                            text=tdocument['text'])
            self.producer.produce('output_documents', json.dumps(res.to_dict()))
        self.consumer.close()


if __name__ == "__main__":
    handler = DocumentsHandler()
    handler.run()