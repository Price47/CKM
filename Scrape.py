from bs4 import BeautifulSoup
import urllib
import requests
import re
import csv

def getData():
    page = requests.get("http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml")
    soup = BeautifulSoup(page.content, 'html.parser')
    hrefs = soup.select("tbody tr td a")
    other_links = soup.select("p a")

    # regex to locate yellow trip and green trip csv's for 2016 only #
    yellow_trip_reg = re.compile('^https:\/\/s3\.amazonaws\.com\/nyc-tlc\/trip\+data\/yellow_tripdata_2016-[0-9]{2}\.csv')
    green_trip_reg = re.compile('^https:\/\/s3\.amazonaws\.com\/nyc-tlc\/trip\+data\/green_tripdata_2016-[0-9]{2}\.csv')
    borough_reg = re.compile('^https:\/\/s3\.amazonaws\.com\/nyc-tlc\/misc\/taxi\+\_zone\_lookup\.csv')

    green_list = []
    yellow_list = []
    borough_ids = []

    for link in other_links:
        if borough_reg.match(link["href"]):
            borough_ids.append(link["href"])

    for link in hrefs:
        if yellow_trip_reg.match(link["href"]):
            yellow_list.append(link["href"])
        if green_trip_reg.match(link["href"]):
           green_list.append(link["href"])


    return {'greenLinks' : green_list, 'yellowLinks' : yellow_list, 'boroughIds' : borough_ids}

def getCSV(csv_link):

    info = requests.get(csv_link)
    decoded_content = info.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    data_list = list(cr)

    return data_list
