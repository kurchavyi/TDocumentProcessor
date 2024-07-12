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
            print(type(self.int_convert_tdocuments_col(res.to_dict())))
            print(res.to_dict())
            self.producer.produce('output_documents', json.dumps(self.int_convert_tdocuments_col(res.to_dict())))
        self.consumer.close()
    
    def int_convert_tdocuments_col(self, tdocument_dict):
        for col in ['pub_date', 'fetch_time', 'first_fetch_time']:
            tdocument_dict[col] = int(tdocument_dict[col])
        return tdocument_dict


if __name__ == "__main__":
    handler = DocumentsHandler()
    handler.run()