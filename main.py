from fastapi import FastAPI, Query, Response, status, Body, Request
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
from pymongo import UpdateOne
from datetime import datetime, time

from serializers import CalculationPayload
import config


app = FastAPI()


@app.on_event('startup')
async def on_startup():
    app.mongo_client = AsyncIOMotorClient(config.MONGODB_URI)

@app.on_event('shutdown')
async def on_shutdown():
    app.mongo_client.close()


async def calculation_task(app: FastAPI, payload: CalculationPayload):
    """
        1. Save into MongoDB
        2. Calculate correlation
        3. Save correlation into MongoDB
    """
    collection = app.mongo_client.get_default_database()[config.MONGODB_METRICS_COLLECTION]
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


@app.post('/calculate')
async def calculate(request: Request, payload: CalculationPayload = Body(...)):
    app = request.app
    asyncio.create_task(calculation_task(app, payload))
    return Response(status_code=status.HTTP_201_CREATED)


@app.get('/correlation')
async def get_correlation(x_data_type: str = Query(...), y_data_type: str = Query(...), user_id: int = Query(...)):
    return Response(status_code=status.HTTP_200_OK)
