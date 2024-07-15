import logging
import decimal

from json import loads, dumps
from confluent_kafka import Consumer, KafkaError, Producer

from processor.api.documents_processor import DocumentsProcessor


class DocumentsHandler:
    """
    The DocumentsHandler class is designed to handle the consumption and
    production of Kafka messages related to document processing.
    It integrates Kafka for message handling and uses
    the DocumentsProcessor class to process documents by inserting
    or updating them in a database.
    """
    def __init__(self, args) -> None:
        logging.info(args)
        self.output_topic = args.output_topic
        self.consumer = Consumer(
            {
                "bootstrap.servers": f"localhost:{args.kafka_port}",
                "group.id": "aaa",
                "auto.offset.reset": "earliest",
            }
        )
        self.consumer.subscribe([f"{args.input_topic}"])
        self.producer = Producer({"bootstrap.servers": f"localhost:{args.kafka_port}"})
        self.processor = DocumentsProcessor(args.pg_url)
    
    def cast_decimal_to_int(self, d):
        for key, value in d.items():
            if isinstance(value, decimal.Decimal):
                d[key] = int(value)
        return d

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
            res = self.cast_decimal_to_int(res.to_dict())
            self.producer.produce(f"{self.output_topic}", dumps(res))
        self.consumer.close()

