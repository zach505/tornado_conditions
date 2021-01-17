#needed to open local files
import os

#needed to read historical Tornado Data
import csv


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
def get_leadup(leadup):
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
    

    #appends the high low and prcp data from the request to the lists shared by all events
    leadup_high +=[item['value'] for item in high_temp]
    leadup_low +=[item['value'] for item in low_temp]
    leadup_prcp += [item['value'] for item in prcp]
    high_date +=[item['date'] for item in high_temp]
    low_date +=[item['date'] for item in low_temp]
    prcp_date +=[item['date'] for item in prcp]
