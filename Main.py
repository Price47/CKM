import mysql.connector

from Scrape import getData
from Settings import database_config, DB_NAME
from StoredProcedures import compress_table
from DatabaseFunctions import (createDatabase, createTables,
    createGeoTable, populateTripData, getTripGeoData)


links = getData()
sql_connection = mysql.connector.connect(**database_config)
cursor = sql_connection.cursor()
cursor2 = sql_connection.cursor(buffered=True)

def main(months):
    """
    populate data in tables

    :param months: how many months of data to include
    :return:
    """
    for i in range(months):
        populateTripData(sql_connection, cursor, cursor2, links, "greenLinks", (11-i))
        populateTripData(sql_connection, cursor, cursor2, links, "yellowLinks", (11-i))
        getTripGeoData(sql_connection, cursor, cursor2)
    compress_table(sql_connection,cursor,"yellow_trip_data", DB_NAME)
    compress_table(sql_connection, cursor, "green_trip_data", DB_NAME)

main(2)

cursor.close()
sql_connection.close()