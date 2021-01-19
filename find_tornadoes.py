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

#locally defined functions
import tornado_func as func



#variables for the number of days before a storm to consider.
leadtime=5
DateDelta = timedelta(days=leadtime)
DateDelta1 = timedelta(days=1)

#initialize informational variables for printouts
procyear=0
num_found=0
num_missing=0



#read tornado history into a list
tornado_hist = func.read_hist()

#read weater station info into a list
daily_station = func.read_station()


#Nested loops to iterate through each tornado event, find the weather stations associated with that county and request NOAA for daily summaries from the lead time to the event.        
for tornado_event in tornado_hist:
    for station in daily_station:
        if tornado_event[1].lower() == station[0].lower():
            end_date = datetime.strptime(tornado_event[3], "%m/%d/%Y") -DateDelta1
            start_date = end_date - DateDelta
            #This if statement checks to see if a new year is being processed and prints some information to the console
            #This is useful for development, but ultimately would be better as a log
            if end_date.year > procyear:
                if procyear>0:
                    print("Number of tornado events with matching weather data: " + str(num_found))
                    print("Number of tornado events with missing weather data: " + str(num_missing))
                    num_found =0
                    num_missing =0
                print("Processing year: " + str(end_date.year))
                procyear = end_date.year
                
            r = requests.get('https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&limit=1000&stationid='+station[1]+'&startdate='+str(start_date.isoformat())+'&enddate='+str(end_date.isoformat()), headers={'token':parms.Token})
            leadup = json.loads(r.text)

            #if checks that the NOAA request returned data
            if(len(leadup) > 0):
                func.get_leadup(leadup, station[1])
                num_found +=1
                func.get_normals(station[1], start_date, end_date)

                
            #else for scenarios where NOAA request did not return data. tries the request again with a backup weather station id
            else:
                r = requests.get('https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&limit=1000&stationid='+station[2]+'&startdate='+str(start_date.isoformat())+'&enddate='+str(end_date.isoformat()), headers={'token':parms.Token})
                leadup = json.loads(r.text)
                #here's a repeat of logic from above. it might be time to make a function
                #if checks that NOAA request returned data
                if(len(leadup) > 0):
                    func.get_leadup(leadup, station[2])
                    func.get_normals(station[2], start_date, end_date)
                    num_found +=1
                else:    
                    print("Could not find weather data for: ", tornado_event[0], tornado_event[1], tornado_event[3])
                    num_missing+=1
            

#prints to the console to finalize yearly information being printed to the console
#like the other prints, useful for development, but would be better as a log
print("Number of tornado events with matching weather data: " + str(num_found))
print("Number of tornado events with missing weather data: " + str(num_missing))


                

