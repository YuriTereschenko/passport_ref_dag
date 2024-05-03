from pydantic import BaseModel, Field, conint, confloat


class RegionRetailInf(BaseModel):
    ap_withou_beer: confloat(ge=0) = Field()
    wine: confloat(ge=0) = Field()
    alcoholic_drink: confloat(ge=0) = Field()
    vodka: confloat(ge=0) = Field()
    cognac: confloat(ge=0) = Field()
    beer: confloat(ge=0) = Field()
    full_ap: confloat(ge=0) = Field()
    rep_year: conint(ge=2023, le=2050) = Field()
    rep_month: conint(ge=1, le=12) = Field()
    region: conint(ge=1, le=95) = Field()
