import mysql.connector
from mysql.connector import errorcode
import requests
from datetime import timedelta
import datetime
import subprocess

from StoredProcedures import *
from Settings import DB_NAME
import Scrape as s


def createDatabase(sql_connection, cursor):
    """

    :param sql_connection: connection to DB
    :param cursor: DB cursor
    :return:
    """
    try:
        sql_connection.database = DB_NAME
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor, DB_NAME)
            sql_connection.database = DB_NAME
        else:
            print(err)
            exit(1)

def createTables(cursor):
    for name, ddl in TABLES.iteritems():
        try:
            print("Creating table {}: ".format(name))
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

def createGeoTable(sql_connection, cursor, cursor2):
    """
    run shell script createGeoTable which converts shape files
    in directory to table in mysql DB. After geo_data table is
    created, use that to populate start of result table

    :param sql_connection: connection to DB
    :param cursor: DB cursor
    :param cursor2: Second DB cursor
    :return:
    """
    subprocess.call(['./createGeoTable.sh'])

    cursor2.execute(get_geo_locations)
    for item in cursor2:
        dt = timedelta(minutes=0)
        while dt != timedelta(hours=23, minutes=30):
            cursor.execute(populate_area_trips, (dt, dt + timedelta(minutes=30), item[0], item[0]))
            dt = dt + timedelta(minutes=30)

    sql_connection.commit()


def getTripGeoData(sql_connection, cursor, cursor2):
    """
    Aggregate data for different zones, inbound and outbound, into
    area_trips table

    :param sql_connection: connection to DB
    :param cursor: DB cursor
    :param cursor2: Second DB cursor
    :return:
    """
    cursor2.execute(get_geo_locations)
    for item in cursor2:
        print item
        dt = timedelta(minutes=0)
        while dt != timedelta(hours=23, minutes=30):
            data = {'start_time': dt,
                    'end_time': dt + timedelta(minutes=30),
                    'location_id': item[0]}
            try:
                cursor.execute(get_green_inbound, data)
            except mysql.connector.Error as err:
                print err
            try:
                cursor2.execute(get_green_outbound, data)
            except mysql.connector.Error as err:
                print err
            try:
                cursor.execute(get_yellow_inbound, data)
            except mysql.connector.Error as err:
                print err
            try:
                cursor2.execute(get_yellow_outbound, data)
            except mysql.connector.Error as err:
                print err

    sql_connection.commit()


def populateTripData(sql_connection,  cursor, cursor2, links, cabLinks, index):
    """
    Download csv link from website and insert into the proper trip data table

    :param sql_connection: connection object to the database
    :param cursor: db cursor
    :param cursor: second cursor
    :param links: links list from scrape function "getData()
    :param cabLinks: either greenLinks or yellowLinks, set to distinguish between
                     green taxi csv's and yellow taxi csv's during data scrape
    :return:
    """
    data = s.getCSV(links[cabLinks][index])
    for d in data:
            try:
                cursor.execute(add_green_trip, {'pickup_datetime' : d[1], 'dropoff_datetime' : d[2],
                                                'pickup_longitude' : d[5], 'pickup_latitude' : d[6],
                                                'dropoff_longitude' : d[7], 'dropoff_latitude' : d[8]})
            except mysql.connector.Error as err:
                print err
            except IndexError as err:
                print err

    sql_connection.commit()

