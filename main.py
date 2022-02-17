from fastapi import Depends, FastAPI, Query, Response, status, Body, Request

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne, ASCENDING

import asyncio
from functools import partial
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from datetime import datetime, time
from scipy.stats import pearsonr

from serializers import CalculationPayload, CorrelationData, DataType, RetrieveCorrelationFilter
import config


app = FastAPI()


@app.on_event('startup')
async def on_startup():
    app.mongo_client = AsyncIOMotorClient(config.MONGODB_URI)
    app.executor = ThreadPoolExecutor(max_workers=5)

@app.on_event('shutdown')
async def on_shutdown():
    app.mongo_client.close()


async def save_metrics(mongo_client, payload: CalculationPayload):
    collection = mongo_client.get_default_database()[config.MONGODB_METRICS_COLLECTION]
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
    await collection.bulk_write(operations)


def get_correlation_from_data(data):
    df = pd.DataFrame(data).set_index('_id')
    df = df.resample('1d').mean()
    df = df.fillna(df.mean())
    return pearsonr(x=df.x, y=df.y)


async def calculate_correlation(mongo_client, payload: CalculationPayload, executor: ThreadPoolExecutor):
    user_id = payload.user_id
    x_data_type, y_data_type = payload.data.x_data_type, payload.data.y_data_type
    result = mongo_client.get_default_database()[config.MONGODB_METRICS_COLLECTION]\
        .aggregate([
            {'$match': {'user_id': user_id, 'data_type': {'$in': [x_data_type, y_data_type]}}},
            {'$project': {
                'date': '$date',
                'x': {'$cond': {'if': {'$eq': ['$data_type', x_data_type]}, 'then': '$value', 'else': None}}, 
                'y': {'$cond': {'if': {'$eq': ['$data_type', y_data_type]}, 'then': '$value', 'else': None}}}},
            {'$group': {'_id': '$date', 'x': {'$max': '$x'}, 'y': {'$max': '$y'}}},
            {'$sort': {'_id': ASCENDING}}
        ])
    data = await result.to_list(None)
    corr, p_value = await (asyncio.get_running_loop().run_in_executor(executor, partial(get_correlation_from_data, data)))

    collection = mongo_client.get_default_database()[config.MONGODB_CORRELATIONS_COLLECTION]
    document = await collection.find_one({'user_id': user_id, 'data_types': {'$all': [x_data_type, y_data_type]}})
    task = collection.update_one(
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
    await task


async def calculation_task(app: FastAPI, payload: CalculationPayload):
    """
        1. Save into MongoDB
        2. Calculate correlation
        3. Save correlation into MongoDB
    """
    await save_metrics(app.mongo_client, payload)
    await calculate_correlation(app.mongo_client, payload, app.executor)


@app.post('/calculate')
async def calculate(request: Request, payload: CalculationPayload = Body(...)):
    app = request.app
    asyncio.create_task(calculation_task(app, payload))
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
