import mysql.connector
from mysql.connector import errorcode

def create_database(cursor, DB_NAME):
    try:
        cursor.execute("set global innodb_file_format=Barracuda;")
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

def compress_table(sql_connection, cursor, table_name, database):
    """
    compress a table and archive it, then remove data from table

    :param sql_connection:
    :param cursor:
    :param table_name:
    :param database:
    :return:
    """
    try:
        cursor.execute("use {0}".format(database))
        cursor.execute("create table `{0}_archive` like `{0}`".format(table_name))
        cursor.execute("alter table `{0}_archive` key_block_size=4 row_format=compressed".format(table_name))
        cursor.execute("insert into `{0}_archive` select * from `{0}`".format(table_name))
        cursor.execute("delete from `{0}`".format(table_name))

        sql_connection.commit()
    except mysql.connector.Error as err:
        print(err)
        exit(1)


TABLES = {}

TABLES['area_trips'] = (
    "CREATE TABLE `area_trips` ("
    " `id` int NOT NULL AUTO_INCREMENT, "
    " `location_id` int,"
    " `area` varchar(100),"
    " `period_start` time,"
    " `period_end` time,"
    " `outbound_to_yellow_zone` int,"
    " `outbound_to_non_yellow_zone` int,"
    " `inbound_from_yellow_zone` int,"
    " `inbound_from_non_yellow_zone` int,"
    " PRIMARY KEY (id)"
    ") ENGINE=InnoDB"
)

TABLES['yellow_trip_data'] = (
    "CREATE TABLE `yellow_trip_data` ("
    " `id` int NOT NULL AUTO_INCREMENT,"
    " `pickup_datetime` datetime,"
    " `dropoff_datetime` datetime,"
    " `pickup_longitude` varchar(250),"
    " `pickup_latitude` varchar(250),"
    " `dropoff_longitude` varchar(250),"
    " `dropoff_latitude` varchar(250),"
    " PRIMARY KEY (id)"
    ") ENGINE=InnoDB"
)

TABLES['green_trip_data'] = (
    "CREATE TABLE `green_trip_data` ("
    " id int NOT NULL AUTO_INCREMENT,"
    " `pickup_datetime` datetime,"
    " `dropoff_datetime` datetime,"
    " `pickup_longitude` varchar(250),"
    " `pickup_latitude` varchar(250),"
    " `dropoff_longitude` varchar(250),"
    " `dropoff_latitude` varchar(250),"
    " PRIMARY KEY (id)"
    ") ENGINE=InnoDB"
)

get_geo_locations = ("SELECT locationid FROM geo_data")

add_borough = ("INSERT INTO boroughs "
               "(borough_id, borough, zone, service_zone) "
               "VALUES (%(borough_id)s, %(borough)s, %(zone)s, %(service_zone)s)"
)

add_yellow_trip = ("INSERT INTO yellow_trip_data"
                   "(pickup_datetime, dropoff_datetime, pickup_longitude, pickup_latitude,"
                   "dropoff_longitude, dropoff_latitude)"
                   "VALUES (%(pickup_datetime)s, %(dropoff_datetime)s, %(pickup_longitude)s, "
                   "%(pickup_latitude)s, %(dropoff_longitude)s, %(dropoff_latitude)s)"
)

add_green_trip = ("INSERT INTO green_trip_data"
                   "(pickup_datetime, dropoff_datetime, pickup_longitude, pickup_latitude, "
                   "dropoff_longitude, dropoff_latitude)"
                   "VALUES (%(pickup_datetime)s, %(dropoff_datetime)s, %(pickup_longitude)s, "
                   "%(pickup_latitude)s, %(dropoff_longitude)s, %(dropoff_latitude)s)"
)

populate_area_trips = ("INSERT INTO area_trips (period_start, period_end, location_id, area)"
                       " SELECT TIME(%s), TIME(%s), %s,"
                       " zone FROM geo_data WHERE locationid = %s"
)

get_yellow_outbound = ("""
                        UPDATE area_trips SET area_trips.outbound_to_yellow_zone =
                        (SELECT COUNT(yellow_trip_data.id)
                         FROM yellow_trip_data, geo_data
                            WHERE ST_CONTAINS(geo_data.shape, Point(yellow_trip_data.dropoff_longitude,
                            yellow_trip_data.dropoff_latitude))
                            and TIME(yellow_trip_data.pickup_datetime) BETWEEN  %(start_time)s AND %(end_time)s
                            and geo_data.locationid = %(location_id)s)
                        WHERE area_trips.location_id = %(location_id)s AND area_trips.period_start >= TIME(%(start_time)s)
                        AND area_trips.period_end <= TIME(%(end_time)s)

                        """)
get_yellow_inbound = ("""
                        UPDATE area_trips SET area_trips.inbound_from_yellow_zone =
                        (SELECT COUNT(yellow_trip_data.id)
                         FROM yellow_trip_data, geo_data
                            WHERE ST_CONTAINS(geo_data.shape, Point(yellow_trip_data.pickup_longitude,
                            yellow_trip_data.pickup_latitude))
                            and TIME(yellow_trip_data.pickup_datetime) BETWEEN  %(start_time)s AND %(end_time)s
                            and geo_data.locationid = %(location_id)s)
                        WHERE area_trips.location_id = %(location_id)s AND area_trips.period_start >= TIME(%(start_time)s)
                        AND area_trips.period_end <= TIME(%(end_time)s)
                        """)
get_green_outbound = ("""
                        UPDATE area_trips SET area_trips.outbound_to_non_yellow_zone =
                        (SELECT COUNT(green_trip_data.id)
                         FROM green_trip_data, geo_data
                            WHERE ST_CONTAINS(geo_data.shape, Point(green_trip_data.dropoff_longitude,
                            green_trip_data.dropoff_latitude))
                            and TIME(green_trip_data.pickup_datetime) BETWEEN  %(start_time)s AND %(end_time)s
                            and geo_data.locationid = %(location_id)s)
                        WHERE area_trips.location_id = %(location_id)s AND area_trips.period_start >= TIME(%(start_time)s)
                        AND area_trips.period_end <= TIME(%(end_time)s)
                        """)
get_green_inbound = ("""
                        UPDATE area_trips SET area_trips.inbound_from_non_yellow_zone =
                        (SELECT COUNT(green_trip_data.id)
                         FROM green_trip_data, geo_data
                            WHERE ST_CONTAINS(geo_data.shape, Point(green_trip_data.pickup_longitude,
                            green_trip_data.pickup_latitude))
                            and TIME(green_trip_data.pickup_datetime) BETWEEN  %(start_time)s AND %(end_time)s
                            and geo_data.locationid = %(location_id)s)
                        WHERE area_trips.location_id = %(location_id)s AND area_trips.period_start >= TIME(%(start_time)s)
                        AND area_trips.period_end <= TIME(%(end_time)s)
                        """
)



