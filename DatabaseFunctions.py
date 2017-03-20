import mysql.connector
from mysql.connector import errorcode
import requests
from datetime import timedelta
import datetime
import subprocess

from StoredProcedures import add_borough, create_database,\
    add_green_trip, add_yellow_trip, populate_area_trips, \
    TABLES, get_green_outbound, get_yellow_outbound, get_green_inbound, \
    get_yellow_inbound
from Settings import database_config, DB_NAME
import Scrape as s



sql_connection = mysql.connector.connect(**database_config)
cursor = sql_connection.cursor()
cursor2 = sql_connection.cursor(buffered=True)

try:
    sql_connection.database = DB_NAME
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        sql_connection.database = DB_NAME
    else:
        print(err)
        exit(1)

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

links = s.getData()

for i in range(4):
    print i


data = {'start_time' : timedelta(minutes=30),
                                    'end_time' : timedelta(minutes=60),
                                    'location_id' : 8}
try:
    cursor.execute(get_green_inbound, data)
    cursor.execute(get_yellow_inbound, data)
    cursor.execute(get_yellow_outbound, data)
except mysql.connector.Error as err:
    print err


def createGeoTable(cursor, cursor2):
    subprocess.call(['./createGeoTable.sh'])

    cursor2.execute("SELECT locationid FROM geo_data")
    for item in cursor2:
        dt = timedelta(minutes=0)
        while dt != timedelta(hours=23, minutes=30):
            cursor.execute(populate_area_trips, (dt, dt + timedelta(minutes=30), item[0], item[0]))
            dt = dt + timedelta(minutes=30)



#
# for link in links['yellowLinks']:
#     print link
# print links['yellowLinks'][0]
# data = s.getCSV(links['yellowLinks'][0])
# for d in data:
#         try:
#             cursor.execute(add_yellow_trip, {'pickup_datetime': d[1], 'dropoff_datetime': d[2],
#                                              'pickup_longitude': d[5], 'pickup_latitude': d[6],
#                                              'dropoff_longitude': d[7], 'dropoff_latitude': d[8]})
#         except mysql.connector.Error as err:
#             print err
#         except IndexError as err:
#             print err
#         except:
#             print "something really fucked up"


def populateTripData(links):

    for link in links['greenLinks']:
        print link
        data = s.getCSV(link)
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

cursor.close()
sql_connection.close()
