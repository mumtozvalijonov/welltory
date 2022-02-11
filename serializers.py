from pydantic import BaseModel, Field, root_validator
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
