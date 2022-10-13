import os
import pyodbc
import sqlalchemy
import time
from shapely import wkt
from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoDataFrame
import pandas as pd
import os
import numpy as np
import yaml
import time
import sys
import transit_service_analyst as tsa
from pathlib import Path
import urllib

def read_from_sde(
    server, database, feature_class_name, version, use_sqlalchemy = True, crs={"init": "epsg:2285"}, is_table=False
):
    """
    Returns the specified feature class as a geodataframe from ElmerGeo.
    Parameters
    ----------
    connection_string : SQL connection string that is read by geopandas
                        read_sql function
    feature_class_name: the name of the featureclass in PSRC's ElmerGeo
                        Geodatabase
    cs: cordinate system
    """
    if use_sqlalchemy:
        connection_string = (
            """mssql+pyodbc://%s/%s?driver=SQL Server?Trusted_Connection=yes"""
            % (server, database)
        )
        # connection_string = '''mssql+pyodbc://%s/%s?driver=ODBC Driver 17 for SQL Server?Trusted_Connection=yes''' % (config['server'], config['database'])
        engine = sqlalchemy.create_engine(connection_string)
        con = engine.connect()
        con.execute("sde.set_current_version {0}".format(version))

    else:
        con = connect(server, database)
        cursor = con.cursor()
        cursor.execute("sde.set_current_version %s", version[1:-1])

    if is_table:
        gdf = pd.read_sql("select * from %s" % (feature_class_name), con=con)
        con.close()

    else:
        if use_sqlalchemy:
            query_string = "select *, Shape.STAsText() as geometry from %s" % (
                feature_class_name
            )
        else:
            geo_col_stmt = (
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME="
                + "'"
                + feature_class_name
                + "'"
                + " AND DATA_TYPE='geometry'"
            )
            geo_col = str(pd.read_sql(geo_col_stmt, con).iloc[0, 0])
            query_string = (
                "SELECT *,"
                + geo_col
                + ".STGeometryN(1).ToString()"
                + " FROM "
                + feature_class_name
            )
        df = pd.read_sql(query_string, con)
        con.close()
        df.rename(columns={"": "geometry"}, inplace=True)

        df["geometry"] = df["geometry"].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry="geometry")
        gdf.crs = crs
        cols = [
            col
            for col in gdf.columns
            if col not in ["Shape", "GDB_GEOMATTR_DATA", "SDE_STATE_ID"]
        ]
        gdf = gdf[cols]

    return gdf

def read_from_elmer_odbc(connection_string, query_string):
    sql_conn = pyodbc.connect(conn_string)
    return pd.read_sql(sql="select * from ofm.v_estimates_2020", con=sql_conn)

def read_from_elmer_sqlalchemy(connection_string, query_string):
    sql_conn = pyodbc.connect(connection_string)
    params = urllib.parse.quote_plus(connection_string)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    return pd.read_sql_query(query_string, engine)
