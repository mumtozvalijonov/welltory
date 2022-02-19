from fastapi import Depends, FastAPI, Query, Response, status, Body, Request
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio

from rabbit import get_channel_pool, publish
from serializers import CalculationPayload, CorrelationData, RetrieveCorrelationFilter
import config


app = FastAPI()


@app.on_event('startup')
async def on_startup():
    app.mongo_client = AsyncIOMotorClient(config.MONGODB_URI)
    app.rmq_channel_pool = get_channel_pool()


@app.on_event('shutdown')
async def on_shutdown():
    app.mongo_client.close()


@app.post('/calculate')
async def calculate(request: Request, payload: CalculationPayload = Body(...)):
    app = request.app
    asyncio.create_task(publish(
        channel_pool=app.rmq_channel_pool,
        payload=payload.json(),
        queue_name=config.RABBITMQ_CALCULATION_QUEUE_NAME)
    )
    return Response(status_code=status.HTTP_201_CREATED)


@app.get('/correlation', response_model=CorrelationData)
async def get_correlation(
    request: Request,
    data_filter: RetrieveCorrelationFilter = Depends(RetrieveCorrelationFilter)
):
    mongo_client = request.app.mongo_client
    collection = mongo_client.get_default_database()[config.MONGODB_CORRELATIONS_COLLECTION]
    document = await collection.find_one({
        'user_id': data_filter.user_id,
        'data_types': {'$all': [data_filter.x_data_type.value, data_filter.y_data_type.value]}
    })
    if document:
        correlation_data = CorrelationData.from_mongo_object(document)
        return correlation_data
    return Response(status_code=status.HTTP_404_NOT_FOUND)
