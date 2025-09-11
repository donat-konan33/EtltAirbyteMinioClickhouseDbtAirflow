
import sys, os
sys.path.append(os.path.dirname(__file__))

from sqlalchemy.orm import Session
from clickhouse_sqlalchemy import make_session
from database import engine
from crud import *
from schemas import *
from typing import List
from fastapi import FastAPI, Depends, HTTPException
import uvicorn

tags = [
        {"name": "users", "description": "Operations with users"},
        {"name": "weather", "description": "Operations with weather data"}
]

app = FastAPI(title="Weather Database", openapi_tags=tags)

def get_db():
   """Helper function which opens a connection to the database and also manages closing the connection"""
   db = make_session(engine)
   try:
       yield db
   finally:
       db.close()


# App landing page
@app.get("/")
def _read_root():
   return {"Weather App": "Running"}

@app.get("/get_data", tags="weather")
def _get_data(db: Session = Depends(get_db)):
   data = get_data(db)
   if not data:
       raise HTTPException(status_code=404, detail="All Data not found")
   return data

@app.get("/date", tags="weather")
def _get_date(db: Session = Depends(get_db)):
   dates = get_date(db)
   if not dates:
       raise HTTPException(status_code=404, detail="Dates not found")
   return dates

@app.get("/get_sunshine_data", tags="weather")
def _get_sunshine_data(db: Session = Depends(get_db)):
    data = get_sunshine_data(db)
    if not data:
        raise HTTPException(status_code=404, detail="Sunshine data not found")
    return data

@app.get("/solar_geo_data/{date}", tags="weather")
def _get_solarenergy_geo_data(date: str, db: Session = Depends(get_db)):
    data = get_solarenergy_geo_data_data(db=db, date=date)
    if not data:
        raise HTTPException(status_code=404, detail="Solar Geo Data not found")
    return data

@app.get("/common_features/{department}", tags="weather")
def _get_tfptwgp(department: str, db: Session = Depends(get_db)):
    data = get_tfptwgp(db=db, department=department)
    if not data:
        raise HTTPException(status_code=404, detail="Common features Data not found")
    return data

@app.get("/get_region_sunshine_data/{region}", tags="weather")
def _get_region_sunshine_data(region: str, db: Session = Depends(get_db)):
    data = get_region_sunshine_data(db=db, region=region)
    if not data:
        raise HTTPException(status_code=404, detail="Region Sunshine data Data not found")
    return data

@app.get("/get_solarenergy_agg_pday/{departement}", tags="weather")
def _get_solarenergy_agg_pday(department: str, db: Session = Depends(get_db)):
    data = get_solarenergy_agg_pday(db=db, region=department)
    if not data:
        raise HTTPException(status_code=404, detail="Daily Solar Aggregating data not found")
    return data

@app.get("/get_entire_region_data/{region}", tags="weather")
def _get_entire_region_data(region: str, db: Session = Depends(get_db)):
    data = get_entire_region_data(db=db, region=region)
    if not data:
        raise HTTPException(status_code=404, detail="Entire Region data not found")
    return data

@app.get("/get_entire_department_data/{department}", tags="weather")
def _get_entire_department_data(department: str, db: Session = Depends(get_db)):
    data = get_entire_department_data(db=db, department=department)
    if not data:
        raise HTTPException(status_code=404, detail="Entire Department data not found")
    return data

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8005)
