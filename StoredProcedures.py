def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

TABLES = {}

TABLES['area_trips'] = (
    "CREATE TABLE `area_trips` ("
    " `id` int NOT NULL AUTO_INCREMENT, "
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

populate_area_trips = ("INSERT INTO area_trips (period_start, period_end, area)"
                       " SELECT TIME(%s), TIME(%s), "
                       " zone FROM geo_data WHERE locationid = %s"
)




