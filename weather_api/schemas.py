from pydantic import BaseModel, PositiveInt, ValidationError, EmailStr
from clickhouse_sqlalchemy import types
from datetime import date
from typing import List

#import logfire
#logfire.configure() # i have to authenticate my app with logfire

#logfire.instrument_pydantic() # to learn mmore

class UserBase(BaseModel):
    email: EmailStr

class CreateUser(UserBase):  # we must add a msg sender fonction to the specified email
    password: str

class ShortWeatherBase(BaseModel):
    dates: date
    department: str

class ShowShortWeather(ShortWeatherBase):
    weekday_name: str
    department: str
    temp: float
    tempmin: float
    tempmax: float
    feelslike: float
    feelslikemin: float
    feelslikemax: float
    descriptions: str

class ShowOtherWeather(ShortWeatherBase):
    reg_name: str
    windspeed: float
    windgust: float
    pressure: float
    solarenergy_kwhpm2: float
    temp: float
    feelslike: float
    precip: float

class GeoDataBase(BaseModel):
    department: str
    geo_point_2d: str
    geojson: str

class ShowRegionSunshine(ShortWeatherBase):
    reg_name: str
    solarenergy_kwhpm2: float
    solarradiation: float

class ShowGeodata(GeoDataBase):
    dates: date
    weekday_name: str
    solarenergy_kwhpm2: float
    solarradiation: float
    reg_name: str
    avg_solarenergy_kwhpm2: float
    avg_solarradiation: float
