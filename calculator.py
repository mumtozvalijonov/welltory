import pika
import pymongo
import logging
import sys

import config
from serializers import CalculationPayload


class Calculator:
    
    def __init__(self):
        self.rmq_params = pika.ConnectionParameters(
            host=config.RABBITMQ_HOST,
            port=config.RABBITMQ_PORT,
            credentials=pika.PlainCredentials(username=config.RABBITMQ_USER, password=config.RABBITMQ_PASSWORD)
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def __enter__(self):
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        self.rmq_connection = pika.BlockingConnection(self.rmq_params)
        self.logger.info("RabbitMQ connection established...")
        self.mongo_client = pymongo.MongoClient(config.MONGODB_URI)
        self.logger.info("MongoDB connection established...")
        return self
    
    def __exit__(self, *_):
        self.rmq_connection.close()
        self.logger.info("RabbitMQ connection closed...")
        self.mongo_client.close()
        self.logger.info("MongoDB connection closed...")
        for handler in self.logger.handlers:
            handler.close()

    def _on_message(self, channel, method_frame, _, body):
        self.logger.info(f"New message: {body}")
        payload = CalculationPayload.parse_raw(body)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def run(self):
        channel = self.rmq_connection.channel()
        channel.basic_consume(config.RABBITMQ_CALCULATION_QUEUE_NAME, self._on_message)
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()


if __name__ == '__main__':
    with Calculator() as calculator:
        calculator.run()
