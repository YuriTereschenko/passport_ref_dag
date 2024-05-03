from pydantic import BaseModel, Field, conint
from datetime import date


class Calendar(BaseModel):
    period_id: conint(gt=0) = Field(gt=0)
    date_end: date = Field()
    date_begin: date = Field()

