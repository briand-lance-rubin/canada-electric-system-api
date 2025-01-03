from pydantic import BaseModel
from datetime import datetime

class BaseRequestModel(BaseModel):
    """
    Base class for request models to ensure consistency in request validation.
    """
    class Config:
        orm_mode = True  # Enable ORM compatibility for database models

class AggregatedDataRequest(BaseRequestModel):
    """
    Request model for aggregated data based on aggregation type.
    """
    aggregation_type: str  # 'monthly' or 'yearly'

class ProcessedDataResponse(BaseModel):
    """
    Response model for processed data retrieval.
    """
    Date: datetime
    Actual_Demand: float
    Forecast_Demand: float
    Forecast_AIL: int
    Actual_AIL: int
    Forecast_AIL_Actual_Difference: float

    class Config:
        orm_mode = True  # Ensure compatibility with ORM models
