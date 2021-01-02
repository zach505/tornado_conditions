#needed to make web requests
import requests

import os

#needed to read historical Tornado Data
import csv

#store the data we get as a dataframe
import pandas as pd

#convert the response as a strcuctured json
import json

#mathematical operations on lists
import numpy as np

#plotting for visualization of dataframe
import matplotlib.pyplot as plt

#parse the datetimes we get from NOAA
from datetime import datetime, timedelta

import parms

#varible for history directory
historydir = 'TornadoHistory'
dailystationdir= 'DailyWeatherStation'
DD = timedelta(days=2)
DD1 = timedelta(days=1)

#initialize lists to store data
tornado_hist = []
daily_station = []
leadup = []
badcount = 0
goodcount = 0

#Iterates through the files in the directory TornadoHistory and adds records from the CSV's downloaded from NOAA's Storm Events Database
#Adds each tornado's data to a list "tornado_hist"
#Further code is going to focus solely on the County and Date, but time of day and magnitude data will be stored as well
for tornadohistfiles in os.listdir(historydir):
    i = 0
    with open(os.path.join(historydir,tornadohistfiles)) as f:
        reader = csv.reader(f)
        for row in reader:
            if i > 0:
                tornado_hist.append(row)
            i+=1
        f.close()

#Adds county weather station info to a list "daily_station"
#This is a place for big improvements. Daily weather stations were chosen manually. A further enhancement would be to use LAT/LONG to find the closest Weather Station.
#NOAA download with detailed station info was fixed width, and profiling the data was not in my scope
for stationfiles in os.listdir(dailystationdir):
    with open(os.path.join(dailystationdir,stationfiles)) as f:
        reader = csv.reader(f)
        for row in reader:
                daily_station.append(row)
        f.close()

#Nested loops to iterate through each tornado event, find the weather stations associated with that county and request NOAA for daily summaries from the lead time to the event.
for tornado_event in tornado_hist:
    for station in daily_station:
        if tornado_event[1].lower() == station[0].lower():
            end_date = datetime.strptime(tornado_event[3], "%m/%d/%Y") -DD1
            start_date = end_date - DD
            r = requests.get('https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&limit=1000&stationid='+station[1]+'&startdate='+str(start_date.year)+'-'+str(start_date.month)+'-'+str(start_date.day)+'&enddate='+str(end_date.year)+'-'+str(end_date.month)+'-'+str(end_date.day), headers={'token':parms.Token})
            if r.status_code == 200:
                goodcount +=1
                print(station[0])
            else:
                r = requests.get('https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&limit=1000&stationid='+station[2]+'&startdate='+str(start_date.year)+'-'+str(start_date.month)+'-'+str(start_date.day)+'&enddate='+str(end_date.year)+'-'+str(end_date.month)+'-'+str(end_date.day), headers={'token':parms.Token})
                if r.status_code == 200:
                    goodcount += 1
                else:
                    badcount += 1
                    

print('good', goodcount, 'bad', badcount)



#Currently only getting good results for about 10% of storms. I am going to sign off and look for 
