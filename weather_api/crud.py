import sys, os
sys.path.append(os.path.dirname(__file__))

from sqlalchemy.orm import Session
import pandas as pd
from sqlalchemy import text, func
from models import mart_newdata, archived_data
from sqlalchemy.ext.asyncio import AsyncSession, AsyncConnection # for later use with FastAPI and async calls


def get_data(db: Session):
    """Get all data from the table"""
    return db.query(mart_newdata).all()


def get_temp_data(db: Session, department: str):
    """Get temperature data like temp, fileslikemin, fileslikemax and feelslike
    """
    return db.query(mart_newdata) \
            .with_entities(
                mart_newdata.c.dates,
                mart_newdata.c.weekday_name,
                mart_newdata.c.department,
                mart_newdata.c.temp,
                mart_newdata.c.tempmin,
                mart_newdata.c.tempmax,
                mart_newdata.c.feelslike,
                mart_newdata.c.feelslikemin,
                mart_newdata.c.feelslikemax,
                mart_newdata.c.descriptions
            ).where(mart_newdata.c.department == department) \
            .order_by(mart_newdata.c.dates).all()


def get_solarenergy_geo_data_data(db: Session, date: str): # faudra voir la Session(importé de sqlachelmy.orm) venant de sqlalchemy Core
    """Get solarenergy_kwhpm2, solarradiation, reg_name, avg_solarenergy_kwhpm2,
    avg_solarradiation"""
    return db.query(mart_newdata) \
        .with_entities(
            mart_newdata.c.dates,
            mart_newdata.c.weekday_name,
            mart_newdata.c.department,
            mart_newdata.c.geo_point_2d,
            mart_newdata.c.geojson,
            mart_newdata.c.solarenergy_kwhpm2,
            mart_newdata.c.solarradiation,
            mart_newdata.c.reg_name,
            mart_newdata.c.avg_solarenergy_kwhpm2,
            mart_newdata.c.avg_solarradiation
        ).where(mart_newdata.c.dates == date).all()


def get_date(db: Session):
    """
	Get Week current date from data recorded in the table
    """
    rows = db.query(mart_newdata.c.dates).distinct().all()
    return [row[0] for row in rows]


def get_tfptwgp(db: Session, department: str):
    """
    Get some interesting features like tfptwgp as :
    Temperature, Feels like, Pecipitation, Wind, Gust and Pressure
    """
    return db.query(mart_newdata) \
        .with_entities(
            mart_newdata.c.dates,
            mart_newdata.c.department,
            mart_newdata.c.reg_name,
            mart_newdata.c.windspeed,
            mart_newdata.c.windgust,
            mart_newdata.c.pressure,
            mart_newdata.c.solarenergy_kwhpm2,
            mart_newdata.c.temp,
            mart_newdata.c.feelslike,
            mart_newdata.c.precip
        ).where(mart_newdata.c.department == department) \
        .order_by(mart_newdata.c.dates).all()


def get_sunshine_data(db: Session):
    """
    Get some interesting features like tfptwgp as :
    Temperature, Feels like, Pecipitation, Wind, Gust and Pressure
    """
    return db.query(mart_newdata.c.dates,
                    mart_newdata.c.reg_name,
                    mart_newdata.c.department,
                    mart_newdata.c.solarenergy_kwhpm2,
                    mart_newdata.c.solarradiation
                    ).all()


def get_region_sunshine_data(db: Session, region:str):
    """
    Get sunshine data for a specific region
    """
    return db.query(mart_newdata) \
        .with_entities(
            mart_newdata.c.reg_name,
            mart_newdata.c.department,
            mart_newdata.c.solarenergy_kwhpm2,
            mart_newdata.c.solarradiation
        ).filter(mart_newdata.c.reg_name == region).all()


def get_solarenergy_agg_pday(db: Session, department:str):
    """
       We take into account the calculation over 8 days as recorded
       Panel area = 2.7 m²
       Panel efficiency = 21.7%
    """
    result = (
    db.query(
        mart_newdata.c.department,
        func.avg(mart_newdata.c.solarenergy_kwhpm2) \
            .label("solarenergy_kwhpm2"),
        (func.avg(mart_newdata.c.solarenergy_kwhpm2) * 2.7) \
            .label("available_solarenergy_kwhc"),
        (func.avg(mart_newdata.c.solarenergy_kwhpm2) * 2.7 * 0.217) \
            .label("real_production_kwhpday"),
    )
    .where(mart_newdata.c.department == department)
    .group_by(mart_newdata.c.department)
    .all()
)
    return result


# we can use execute with text query for more complex queries
# checck test from database.py
def get_entire_department_data(db: Session, department:str):
    """
       Get local entire data for a department from data agregated so far
    """
    return db.query(
                      archived_data.c.dates,
                      archived_data.c.department,
                      func.round(archived_data.c.solarenergy_kwhpm2, 1) \
                          .label("solarenergy_kwhpm2"),
                      func.round(archived_data.c.solarradiation, 1) \
                          .label("solarradiation"),
                      func.round(archived_data.c.temp, 1) \
                          .label("temp"),
                      func.round(archived_data.c.precip, 1) \
                          .label("precip"),
                      func.round(archived_data.c.uvindex, 1) \
                          .label("uvindex")
                      ).filter(archived_data.c.department == department) \
                          .order_by(archived_data.c.dates).all()


def get_entire_region_data(db: Session, region:str):
    """
    Get local entire data for a region from data agregated so far
    """
    return db.query(
        archived_data.c.dates,
        archived_data.c.reg_name,
        func.round(func.avg(archived_data.c.solarenergy_kwhpm2), 1) \
                .label("solarenergy_kwhpm2"),
            func.round(func.avg(archived_data.c.solarradiation), 1) \
                .label("solarradiation"),
            func.round(func.avg(archived_data.c.temp), 1) \
                .label("temp"),
            func.round(func.avg(archived_data.c.precip), 1) \
                .label("precip"),
            func.round(func.avg(archived_data.c.uvindex), 1) \
                .label("uvindex")
                      ) \
            .where(archived_data.c.reg_name == region) \
            .group_by(archived_data.c.dates, archived_data.c.reg_name) \
            .order_by(archived_data.c.dates, archived_data.c.reg_name).all()
