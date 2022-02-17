import os
from dotenv import load_dotenv
from pathlib import Path


BASE_DIR = Path(__file__).parent
env_file = BASE_DIR/'.env'

if env_file.exists():
    load_dotenv(env_file)


MONGODB_URI = os.environ['MONGODB_URI']
MONGODB_METRICS_COLLECTION = os.environ['MONGODB_METRICS_COLLECTION']
MONGODB_CORRELATIONS_COLLECTION = os.environ['MONGODB_CORRELATIONS_COLLECTION']

RABBITMQ_HOST = os.environ['RABBITMQ_HOST']
RABBITMQ_PORT = os.environ['RABBITMQ_PORT']
RABBITMQ_USER = os.environ['RABBITMQ_DEFAULT_USER']
RABBITMQ_PASSWORD = os.environ['RABBITMQ_DEFAULT_PASS']
RABBITMQ_CALCULATION_QUEUE_NAME = os.environ['RABBITMQ_CALCULATION_QUEUE_NAME']
