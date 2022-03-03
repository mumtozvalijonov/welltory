import pika
import pymongo
from pymongo import UpdateOne, ASCENDING
import logging
import sys
from datetime import datetime, time
import pandas as pd
from scipy.stats import pearsonr

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
        try:
            self.logger.info(f"New message: {body}")
            payload = CalculationPayload.parse_raw(body)

            self._save_metrics(payload)
            self._calculate_correlation(payload)        
            
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        except Exception as e:
            self.logger.error(e)

    def _save_metrics(self, payload: CalculationPayload):
        collection = self.mongo_client.get_default_database()[config.MONGODB_METRICS_COLLECTION]
        operations = []
        for x, y in zip(payload.data.x, payload.data.y):
            x_doc, y_doc = x.dict(), y.dict()
            x_doc['date'], y_doc['date'] = datetime.combine(x.date, time(0, 0)), datetime.combine(y.date, time(0, 0))
            x_doc['user_id'], y_doc['user_id'] = payload.user_id, payload.user_id
            x_doc['data_type'], y_doc['data_type'] = str(payload.data.x_data_type), str(payload.data.y_data_type)
            operations.extend([
                UpdateOne(
                    {
                        'user_id': x_doc['user_id'],
                        'data_type': x_doc['data_type'],
                        'date': x_doc['date']
                    },
                    {'$set': x_doc},
                    upsert=True
                ),
                UpdateOne(
                    {
                        'user_id': y_doc['user_id'],
                        'data_type': y_doc['data_type'],
                        'date': y_doc['date']
                    },
                    {'$set': y_doc},
                    upsert=True
                )
            ])
        collection.bulk_write(operations)

    def _calculate_correlation(self, payload: CalculationPayload):
        user_id = payload.user_id
        x_data_type, y_data_type = payload.data.x_data_type, payload.data.y_data_type
        result = self.mongo_client.get_default_database()[config.MONGODB_METRICS_COLLECTION]\
            .aggregate([
                {'$match': {'user_id': user_id, 'data_type': {'$in': [x_data_type, y_data_type]}}},
                {'$project': {
                    'date': '$date',
                    'x': {'$cond': {'if': {'$eq': ['$data_type', x_data_type]}, 'then': '$value', 'else': None}}, 
                    'y': {'$cond': {'if': {'$eq': ['$data_type', y_data_type]}, 'then': '$value', 'else': None}}}},
                {'$group': {'_id': '$date', 'x': {'$max': '$x'}, 'y': {'$max': '$y'}}},
                {'$sort': {'_id': ASCENDING}}
            ])
        data = list(result)
        df = pd.DataFrame(data).set_index('_id')
        df = df.resample('1d').mean()
        df = df.fillna(df.mean())
        corr, p_value = pearsonr(x=df.x, y=df.y)

        collection = self.mongo_client.get_default_database()[config.MONGODB_CORRELATIONS_COLLECTION]
        document = collection.find_one({'user_id': user_id, 'data_types': {'$all': [x_data_type, y_data_type]}})
        collection.update_one(
            {'_id': document['_id']}, 
            {
                '$set': {
                    'correlation': corr,
                    'p_value': p_value
                }
            }
        ) if document else collection.insert_one({
                                'user_id': user_id,
                                'data_types': [x_data_type, y_data_type],
                                'correlation': corr,
                                'p_value': p_value
                            })

    def run(self):
        channel = self.rmq_connection.channel()
        channel.queue_declare(config.RABBITMQ_CALCULATION_QUEUE_NAME, auto_delete=False)
        channel.basic_consume(config.RABBITMQ_CALCULATION_QUEUE_NAME, self._on_message)
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()


if __name__ == '__main__':
    with Calculator() as calculator:
        calculator.run()
