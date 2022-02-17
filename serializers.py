from fastapi import Query, HTTPException, status
from pydantic import BaseModel, root_validator
from datetime import date
from typing import List
from enum import Enum


class DataType(Enum):
    AVG_HEARTBEAT = 'avg_heartbeat'
    CALORIES_CONSUMED = 'calories_consumed'
    SLEEP_HOURS = 'sleep_hours'
    MORNING_PULSE = 'morning_pulse'


class MetricDataPoint(BaseModel):
    date: date
    value: float


class MetricsData(BaseModel):
    x_data_type: DataType
    y_data_type: DataType
    x: List[MetricDataPoint]
    y: List[MetricDataPoint]

    @root_validator(pre=True)
    def _validate_data_types_are_different(cls, values):
        x_data_type, y_data_type = values.get('x_data_type'), values.get('y_data_type')
        assert x_data_type != y_data_type, 'x_data_type and y_data_type MUST be different'
        x_values, y_values = values.get('x'), values.get('y')
        assert len(x_values) == len(y_values), 'x and y lengths mismatch'
        return values

    class Config:  
        use_enum_values = True

class CalculationPayload(BaseModel):
    user_id: int
    data: MetricsData


class Correlation(BaseModel):
    value: float
    p_value: float


class CorrelationData(BaseModel):
    user_id: int
    data_types: List[DataType]
    correlation: Correlation
    
    @classmethod
    def from_mongo_object(cls, obj):
        return cls(
            user_id=obj['user_id'],
            data_types=obj['data_types'],
            correlation=Correlation(
                value=obj['correlation'],
                p_value=obj['p_value']
            )
        )


class RetrieveCorrelationFilter:

    def __init__(
        self,
        x_data_type: DataType = Query(...),
        y_data_type: DataType = Query(...),
        user_id: int = Query(...)
    ):
        if x_data_type == y_data_type:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='x_data_type must be different from y_data_type')
        self.x_data_type = x_data_type
        self.y_data_type = y_data_type
        self.user_id = user_id
