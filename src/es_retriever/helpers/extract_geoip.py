from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType


def extract_lat(geo_row):

    if "location" in geo_row and geo_row.location is not None:
        lat = geo_row.location.lat
    else:
        lat = None

    return lat


def extract_lon(geo_row):
    if "location" in geo_row and geo_row.location is not None:
        lon = geo_row.location.lon
    else:
        lon = None

    return lon


def extract_country(geo_row):

    if "country_name" in geo_row and geo_row.country_name is not None:
        country = geo_row.country_name
    else:
        country = None

    return country


extract_lat_udf = F.udf(extract_lat, FloatType())
extract_lon_udf = F.udf(extract_lon, FloatType())
extract_country_udf = F.udf(extract_country, StringType())

