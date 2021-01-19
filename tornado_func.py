#needed to open local files
import os

#needed to read historical Tornado Data
import csv

#needed to make web requests
import requests

#local parameter file to keep sensitive and user specific information
import parms

#needed to convert the response as a structured json
import json

#needed to parse the datetimes sent/received to/from NOAA
from datetime import datetime, timedelta

#variables for local directories
historydir = 'TornadoHistory'
dailystationdir= 'DailyWeatherStation'




#initialize lists that will be used to store leadup information for all events
leadup_high = []
leadup_low = []
leadup_prcp = []

high_date = []
low_date = []
prcp_date = []

norm_high  = []
norm_high_sd  = []
norm_low = []
norm_low_sd = []

norm_high_dt = []
norm_high_sd_dt = []
norm_low_dt = []
norm_low_sd_dt = []



weather_station = []


#Iterates through the files in the directory TornadoHistory and adds records from the CSV's downloaded from NOAA's Storm Events Database
#Adds each tornado's data to a list "tornado_hist"
def read_hist():
    tornado_hist = []
    for tornadohistfiles in os.listdir(historydir):
        i = 0
        with open(os.path.join(historydir,tornadohistfiles)) as f:
            reader = csv.reader(f)
            for row in reader:
                if i > 0:
                    tornado_hist.append(row)
                i+=1
            f.close()
    return tornado_hist

#Adds county weather station info to a list "daily_station"
#This is a place for big improvements. Daily weather stations were chosen manually. A further enhancement would be to use LAT/LONG to find the closest Weather Station.
#UPDATE: I see bounded box as a great path forward for requesting based on the lat/long of the tornado.
#       Some tornado events do not have lat/long, but being able to compare SW of the event to NE seems useful
def read_station():
    daily_station = []
    for stationfiles in os.listdir(dailystationdir):
        with open(os.path.join(dailystationdir,stationfiles)) as f:
            reader = csv.reader(f)
            for row in reader:
                daily_station.append(row)
            f.close()
    return daily_station

#This function processes a NOAA request for historical data leading up to a tornado
#It adds the daily high, low and percipitation data for each day leading up to the day of a tornado
#If more data points become necessary they should be added to this function
#Potentially necessary information: days between weather and tornado, weather station id, 
def get_leadup(leadup, stationid):
    #identifies all highs, lows and prcp data from the request
    high_temp = [item for item in leadup['results'] if item['datatype'] == 'TMAX']
    low_temp = [item for item in leadup['results'] if item['datatype'] == 'TMIN']
    prcp = [item for item in leadup['results'] if item['datatype'] == 'PRCP']
    

    global leadup_high
    global leadup_low
    global leadup_prcp

    global high_date
    global low_date
    global prcp_date
    global weather_station
    

    #appends the high low and prcp data from the request to the lists shared by all events
    leadup_high +=[item['value'] for item in high_temp]
    leadup_low +=[item['value'] for item in low_temp]
    leadup_prcp += [item['value'] for item in prcp]
    high_date +=[item['date'] for item in high_temp]
    low_date +=[item['date'] for item in low_temp]
    prcp_date +=[item['date'] for item in prcp]

    #appends the stationid to the list shared by all events
    weather_station +=[stationid for item in high_temp]
    
def get_normals(stationid, start_date, end_date):

    #the leadup period may have crossed from January back to December.
    #If the years are not the same, recursively call get_normals from Jan 1 to passed end date
    #Continue this call to get_normals from the passed start date to 12/31
    if start_date.year != end_date.year:
        new_year_start_date = datetime.strptime('01/01/'+str(end_date.year), "%m/%d/%Y")
        get_normals(stationid, new_year_start_date, end_date)
        #Searching on 30 year normals is done with the year set to 2010. Code reintereprets the day range leading up to a tornado into 2010 date range
        norm_start = datetime.strptime(str(start_date.month) + "/"+str(start_date.day) +"/"+ "2010", "%m/%d/%Y")
        norm_end = datetime.strptime('12/31/2010', "%m/%d/%Y")
        print("weird edge case found")
    else:
        #Searching on 30 year normals is done with the year set to 2010. Code reintereprets the day range leading up to a tornado into 2010 date range
        norm_start = datetime.strptime(str(start_date.month) + "/"+str(start_date.day) +"/"+ "2010", "%m/%d/%Y")
        norm_end = datetime.strptime(str(end_date.month) + "/"+str(end_date.day) +"/"+ "2010", "%m/%d/%Y")

    
    r2 = requests.get('https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=NORMAL_DLY&limit=1000&stationid='+stationid+'&startdate='+str(norm_start.isoformat())+'&enddate='+str(norm_end.isoformat()), headers={'token':parms.Token})
    norm = json.loads(r2.text)

    if len(norm) > 0:
        normal_high = [item for item in norm['results'] if item['datatype']== 'DLY-TMAX-NORMAL'] 
        normal_high_std_dev = [item for item in norm['results'] if item['datatype']== 'DLY-TMAX-STDDEV']
        normal_low = [item for item in norm['results'] if item['datatype']== 'DLY-TMIN-NORMAL']
        normal_low_std_dev = [item for item in norm['results'] if item['datatype']== 'DLY-TMIN-STDDEV']

        global norm_high
        global norm_high_sd
        global norm_low
        global norm_low_sd

        global norm_high_dt
        global norm_high_sd_dt
        global norm_low_dt
        global norm_low_sd_dt

        norm_high +=[item['value'] for item in normal_high]
        norm_low +=[item['value'] for item in normal_low]
        norm_high_sd += [item['value'] for item in normal_high_std_dev]
        norm_low_sd += [item['value'] for item in normal_low_std_dev]

        norm_high_dt +=[item['date'] for item in normal_high]
        norm_low_dt +=[item['date'] for item in normal_low]
        norm_high_sd_dt += [item['date'] for item in normal_high_std_dev]
        norm_low_sd_dt += [item['date'] for item in normal_low_std_dev]



    
