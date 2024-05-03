from pydantic import BaseModel, Field, conint


class RegionLicInf(BaseModel):
    rpo: conint(ge=0) = Field()
    rpa_rpo: conint(ge=0) = Field()
    cancelled_lic: conint(ge=0) = Field()
    period_id: conint(gt=0, le=20) = Field()
    region: conint(ge=1, le=95) = Field()
