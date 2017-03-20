#!/bin/sh
ogr2ogr -f MySql "MySQL:TripData,host=localhost,user=root,password=root" -nln geo_data -update -overwrite -lco engine=MYISAM ./taxi_zones/taxi_zones.shp
