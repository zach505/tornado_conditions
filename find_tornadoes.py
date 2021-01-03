#Authored by Zach Dicks on 01-02-2021
#
#   The purpose of this project is to better understand the weather in a region leading up to the appearance of a tornado.
#   I chose to focus on the state of Mississippi. It has a high level of tornado activity, but would limit the scope of manual work due to its size.
#
#   Process:
#       1. Gather all tornado history since 1995 from NOAA's sever weather database through bulk request and CSV exports. Read these into a list.
#       2. Create a list of weather stations that cover the history of a region from 1995-2020.
#           2a. I gathered a list of all counties in Mississippi. Using NOAA's weather station finder tool I chose stations for each county. I entered the station ID into a CSV at a county level.
#       3. Iterate through the events to find the weather stations associated with the county where the event happened.
#       4. Using NOAA's API, send requests for a parameterized period of time before the event for the weather station associated with the county.
#       5. Store the results of the requests in data frames
#       6. Perform analysis on the dataframe comparing temperatures and percipitation before a tornado to historical averages.
#
#
#   The project combines bulk data collection, requests to the NOAA API and a manually created lookup.
#


#needed to make web requests
import requests

#needed to open local files
import os

#needed to read historical Tornado Data
import csv

#needed to store the data we get as a dataframe
import pandas as pd

#needed to convert the response as a structured json
import json

#needed to mathematical operations on lists
import numpy as np

#needed to plot for visualization of dataframe
import matplotlib.pyplot as plt

#needed to parse the datetimes sent/received to/from NOAA
from datetime import datetime, timedelta

#local parameter file to keep sensitive and user specific information
import parms

#variables for local directories
#variables for the number of days before a storm to consider
historydir = 'TornadoHistory'
dailystationdir= 'DailyWeatherStation'

#variables for the number of days before a storm to consider.
leadtime=5
DateDelta = timedelta(days=leadtime)
DateDelta1 = timedelta(days=1)

#initialize lists to store csv data
tornado_hist = []
daily_station = []

#initialize lists that will be used to identify datatype from our NOAA requests
high_temp = []
low_temp = []
prcp = []

#initialize lists that will be used to store leadup information for all events
leadup_high = []
leadup_low = []
leadup_prcp = []

high_date = []
low_date = []
prcp_date = []

#debugging variables to understand how many requests are successful
badcount = 0
goodcount = 0

#Iterates through the files in the directory TornadoHistory and adds records from the CSV's downloaded from NOAA's Storm Events Database
#Adds each tornado's data to a list "tornado_hist"
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
#UPDATE: I see bounded box as a great path forward for requesting based on the lat/long of the tornado.
#       Some tornado events do not have lat/long, but being able to compare SW of the event to NE seems useful
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
            end_date = datetime.strptime(tornado_event[3], "%m/%d/%Y") -DateDelta1
            start_date = end_date - DateDelta
            #Commented code is WIP to query NOAA for weather stations based on Lat/Long
            #if tornado_event[30] != '' and tornado_event[31] != '':
            #    lat_range_low = float(tornado_event[30]) - 1
            #    lat_range_high = float(tornado_event[30]) + 1
            #    lon_range_low = float(tornado_event[31]) - 1
            #    lon_range_high = float(tornado_event[31]) + 1
            #r = requests.get('https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&limit=1000&startdate='+str(start_date.isoformat())+'&enddate='+str(end_date.isoformat())+'&boundingBox='+str(lat_range_low)+','+str(lon_range_low)+','+str(lat_range_high)+','+str(lon_range_high), headers={'token':parms.Token})
            r = requests.get('https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&limit=1000&stationid='+station[1]+'&startdate='+str(start_date.isoformat())+'&enddate='+str(end_date.isoformat()), headers={'token':parms.Token})
#            if len(r.content) > 0:
#                goodcount +=1
#                print(station[0])
#            else:
                #When compiling the list of weather stations, one limitation was finding a single weather station that covered the entire date range 1995-2020
                #In this situation I listed multiple weather stations to better cover the time frame.
                #If there is no result in the first request, this request is sent with a secondary station ID
#                r = requests.get('https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&limit=1000&stationid='+station[2]+'&startdate='+str(start_date.year)+'-'+str(start_date.month)+'-'+str(start_date.day)+'&enddate='+str(end_date.year)+'-'+str(end_date.month)+'-'+str(end_date.day), headers={'token':parms.Token})
#                if len(r.content) > 0:
#                    goodcount += 1
#                else:
#                    print("Could not find weather data for: ", tornado_event[0], tornado_event[1], tornado_event[3])
            leadup = json.loads(r.text)

            if(len(leadup) > 0):
                #identifies all highs, lows and prcp data from the request
                high_temp = [item for item in leadup['results'] if item['datatype'] == 'TMAX']
                low_temp = [item for item in leadup['results'] if item['datatype'] == 'TMIN']
                prcp = [item for item in leadup['results'] if item['datatype'] == 'PRCP']

                #appends the high low and prcp data from the request to the lists shared by all events
                leadup_high +=[item['value'] for item in high_temp]
                leadup_low +=[item['value'] for item in low_temp]
                leadup_prcp += [item['value'] for item in prcp]
                high_date +=[item['date'] for item in high_temp]
                low_date +=[item['date'] for item in low_temp]
                prcp_date +=[item['date'] for item in prcp]
            else:
                print("Could not find weather data for: ", tornado_event[0], tornado_event[1], tornado_event[3])
                
            
print('good', goodcount, 'bad', badcount)


