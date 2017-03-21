import mysql.connector

from Scrape import getData
from DatabaseFunctions import (createDatabase, createTables,
    createGeoTable)
from Settings import database_config

links = getData()
sql_connection = mysql.connector.connect(**database_config)
cursor = sql_connection.cursor()
cursor2 = sql_connection.cursor(buffered=True)

# Setup MySQL database and tables ##
createDatabase(sql_connection, cursor)
createTables(cursor2)
createGeoTable(sql_connection, cursor, cursor2)