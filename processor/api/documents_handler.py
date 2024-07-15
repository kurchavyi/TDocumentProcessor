import logging

from json import loads, dumps
from confluent_kafka import Consumer, KafkaError, Producer

from processor.documents_processor import DocumentsProcessor


class DocumentsHandler:
    """
    The DocumentsHandler class is designed to handle the consumption and
    production of Kafka messages related to document processing.
    It integrates Kafka for message handling and uses
    the DocumentsProcessor class to process documents by inserting
    or updating them in a database.
    """
    def __init__(self) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": "aaa",
                "auto.offset.reset": "earliest",
            }
        )
        self.consumer.subscribe(["new_input_documents"])

        self.producer = Producer({"bootstrap.servers": "localhost:9092"})

        self.processor = DocumentsProcessor()

    def run(self) -> None:
        """
        Run the document handling process, continuously polling for new messages,
        processing them, and producing the results to an output topic.
        """
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(msg.error())
                    break
            dict_document = loads(msg.value().decode("utf-8"))
            logging.info("Received message: {}".format(msg.value().decode("utf-8")))
            res = self.processor.process_document(dict_document)
            self.producer.produce("output_documents", dumps(res.to_dict()))
        self.consumer.close()


if __name__ == "__main__":
    handler = DocumentsHandler()
    handler.run()
