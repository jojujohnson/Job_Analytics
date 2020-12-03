import os
import sys
import re
import time
import pytz 
import urllib.request
#import shutil
#import glob
from datetime import datetime 
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from geopy.geocoders import Nominatim
from db_objects_operations import *
import configparser
import psycopg2

config = configparser.ConfigParser()
config.read_file(open('/home/ubuntu/JobAnalytics/Config/config.cfg'))

#Get connection and key values from config file

PSQL_DB          = config.get("POSTGRESQL","PSQL_DB")
PSQL_DB_USER     = config.get("POSTGRESQL","PSQL_DB_USER")
PSQL_DB_PASSWORD = config.get("POSTGRESQL","PSQL_DB_PASSWORD")
PSQL_PORT        = config.get("POSTGRESQL","PSQL_PORT")
PSQL_HOSTNAME    = config.get("POSTGRESQL","PSQL_HOSTNAME")

def create_database():
    """
    - Creates and connects 
    - Returns the connection and cursor 
    """
    
    # connect to default database
    connection = psycopg2.connect(database = PSQL_DB,
                                  user = PSQL_DB_USER,
                                  password = PSQL_DB_PASSWORD,
                                  host = PSQL_HOSTNAME,
                                  port = PSQL_PORT)

    cursor = connection.cursor()

    return cursor, connection

def validate_job_dimension(cursor,connection,options,DRIVER_PATH):
    print('@@@@@@@ Executing job_validation Function @@@@@@@')

    # select max of action_id + 1 and current db date time from action_log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    cursor.execute(action_log_select_max_action_id_plus_1_current_date_time)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    action_id=split_data[0].replace("(","")
    split_data=re.split(",",str(results[0]))
    start_time=split_data[1].replace(")","")

    cursor.execute(job_data_dimension_validation_select_validation_pending)
    results = cursor.fetchall()

    data=[]
    counter=0

    for record in results:
        print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print(record)

        job_id=record[0]
        job_url=record[1]
        filedata='NO_DATA'

        if not job_url=='URL_EXCEPTION_FOUND' or not job_url=='URL_DATA_NOT_AVAILABLE': 
           try:
                 req = urllib.request.Request(job_url)
                 req.add_header('Accept', 'application/json,application/xml')
                 file=urllib.request.urlopen(req).read().decode('utf-8')
                 filedata=file.replace('[','\n')
                 filedata=filedata.replace('(','~')
                 filedata=filedata.replace(')','~')

                 #driver1=webdriver.Chrome(options=options, executable_path=DRIVER_PATH) 

                 #driver1.get(job_url)
                 #time.sleep(1)
                 #soup = BeautifulSoup(driver1.page_source, 'lxml') 
                 #URLData=soup.prettify()
                 URLDataCheck=re.search("Posted", filedata)                
                 if URLDataCheck:
                    print("                                                        <-- URL_RENDER_SUCCESS -->")
                    URLDataCheck='URL_RENDER_SUCCESS'
                 else:
                    print("                                                                 <-- URL_RENDER_FAILED-->")
                    URLDataCheck='URL_RENDER_FAILED'
           except:
                 print("                                                              <-- URL_RENDER_FAILED -->")
                 URLDataCheck='URL_RENDER_FAILED'
           #driver1.quit()
        else:
           print("URL NOT FOUND")

        job_data=filedata
        job_data=job_data.replace("  ","")
        job_url_render_status=URLDataCheck
        job_validation_status='Validation_Completed'
        if URLDataCheck=='URL_RENDER_FAILED':
           job_validation_status='Validation_Failed'

        job_type=''
        job_department=''
        print("<-- JOB TYPE -:", job_type ,"-->")
        print("<-- JOB DEPARTMENT -:", job_department ,"-->")
        print("<-- JOB DATA -:", job_data ,"-->")
        print("<-- JOB URL RENDER STATUS -:", job_url_render_status ,"-->") 
        print("<-- JOB VALIDATION STATUS -:", job_validation_status ,"-->")
   
        job_dimension_data = [job_type, job_department, str(job_data), job_url_render_status, job_validation_status, job_id]
        cursor.execute(job_data_dimension_table_update, job_dimension_data)
        connection.commit()
        counter+=1

    # select total record count from job_dimension time >= start_time ~ db_objects.py ~ SELECT TABLE OPERATIONS
    filter_data = [start_time,start_time]
    cursor.execute(job_dimension_select_create_update_count_gteq_start_time,filter_data)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    update_record_count=split_data[1].replace(")","")

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS 
    object_name='JOB_DIMENSION_VALIDATE'
    action_type='UPDATE'
    action_func_total_counter=counter
    action_log_data = [action_id, object_name, action_type, start_time, update_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()

    # select details for current run in Action Log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    action_log_data = [action_id]
    cursor.execute(action_log_select_record,action_log_data)
    results = cursor.fetchall()
    print("AL   : Action_Log                     : Total Record Count           : ",results)


def validate_location_dimension(cursor,connection):
    print('@@@@@@@ Executing location_validation Function @@@@@@@')
    from datetime import datetime

    # select max of action_id + 1 and current db date time from action_log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    cursor.execute(action_log_select_max_action_id_plus_1_current_date_time)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    action_id=split_data[0].replace("(","")
    split_data=re.split(",",str(results[0]))
    start_time=split_data[1].replace(")","")

    cursor.execute(location_dimension_validation_select_validation_status_pending)
    results = cursor.fetchall()

    geolocator = Nominatim(user_agent="JobCheck")
    data=[]
    counter=0
    for record in results:
        print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~")
        FullLocation=record[1]
        print(FullLocation)    

        latitude=0.0
        longitude=0.0
        LocationArr='Direct_Hit'
        LocationFound='Direct_Hit'
        LocationHit='Direct_Hit'
        LocationRev='Direct_Hit'
        GmapLocation='Not Generated'
        location_validation_status='Validation_Completed'

        print("<-- LOCATION ID -: ",record[0],"-->")

        try:
         location = geolocator.geocode(FullLocation)
         latitude=location.latitude 
         longitude=location.longitude
         GmapLocation='https://www.google.com/maps?q='+str(location.latitude)+','+str(location.longitude)
         location_validation_status='Validation_Completed'
         print("<-- ACTUAL LOCATION -: ",location,"-->")
         print("<-- CORDINATES -: ", location.latitude, location.longitude,"-->")
         print("<-- GMAP LINK -:", GmapLocation ,"-->")
        except:
         Location=FullLocation.split(" ")
         for l in range(0,len(Location)-1):
           if Location[l]!='':
           #ignore keywords
            try:
                 location = geolocator.geocode(Location[l])
                 locStr=str(location.latitude)+','+str(location.longitude)
                 latitude=location.latitude 
                 longitude=location.longitude
                 LocationArr=Location
                 LocationFound=Location[l]
                 LocationHit=l
                 #LocationRev=geolocator.reverse(locStr)
                 GmapLocation='https://www.google.com/maps?q='+str(location.latitude)+','+str(location.longitude)
                 print("<-- EXCEPTION LOCATION -: ",location,"-->")
                 print("<-- CORDINATES -:", location.latitude, location.longitude ,"-->")
                 #print("<-- LOCATION REV -:", LocationRev ,"-->")
                 print("<-- LOCATION FOUND -:", LocationFound ," | LOCATION HIT -:", LocationHit ,"-->")
                 print("<-- GMAP LINK -:", GmapLocation ,"-->")
                 location_validation_status='Exception_Found'
                 break;
            except:
                 print("Location Retry")
                 location_validation_status='Validation_Failed'
                 
        if location:
           try:
                 tf = TimezoneFinder()
                 TimeZone=tf.timezone_at(lng=longitude, lat=latitude)
                 #print("<-- TimeZone -: ",TimeZone,"-->")
         
                 tZone = pytz.timezone(TimeZone) 
                 #print("Default Format : ",  tZone)

                 datetime = datetime.now(tZone) 
                 TimeNow=datetime.strftime('%Y:%m:%d %H:%M:%S')
                 TimeZone2=datetime.strftime('%Z %z')
    
                 print("<-- TimeZone -: ",TimeZone," ",TimeZone2,"-->")
                 print("<-- DATE & TIME (CURRENT) -: ",  datetime.strftime('%Y:%m:%d %H:%M:%S'),"-->")

           except:
                 print('TimeZone Failed')
        else:
                 location_validation_status='Exception_Found'
                 LocationArr='No_Hit'
                 LocationFound='No_Hit'
                 LocationHit='No_Hit'
                 LocationRev='No_Hit'
                 TimeZone=''
                 TimeNow=''

        location_id=record[0]
        location_actual=str(location)
        location_array=LocationArr
        location_array_hit_keyword=LocationFound
        location_arr_hit_position=LocationHit
        location_reverse_value=str(LocationRev)
        gmap_link=GmapLocation
        timezone=str(TimeZone)+' '+str(TimeZone2)
        timenow=TimeNow
    
        location_status=0
        if location_array_hit_keyword=='Direct_Hit':
           location_status=1
        
        print("<-- LOCATION STATUS -:", location_status ,"-->") 
        print("<-- VALIDATION STATUS -:", location_validation_status ,"-->")

        location_dimension_data = [location_actual,latitude,longitude,location_array,location_array_hit_keyword,location_arr_hit_position,location_reverse_value,gmap_link,timezone,timenow,location_validation_status,location_status,location_id]
        try:
         cursor.execute(location_dimension_table_update, location_dimension_data)
        except:
         print ("<-- INSIDE EXCEPTION BLOCK -->")
         location_actual='UNSUPPORTED_CHARACTER_SET'
         location_reverse_value='UNSUPPORTED_CHARACTER_SET'
         location_dimension_data = [location_actual,latitude,longitude,location_array,location_array_hit_keyword,location_arr_hit_position,location_reverse_value,gmap_link,timezone,timenow,location_validation_status,location_status,location_id]
         cursor.execute(location_dimension_table_update, location_dimension_data)
   
        connection.commit()
        counter+=1
       
 
    # select total record count from location_dimension time >= start_time ~ db_objects.py ~ SELECT TABLE OPERATIONS
    filter_data = [start_time,start_time]
    cursor.execute(location_dimension_select_create_update_count_gteq_start_time,filter_data)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    update_record_count=split_data[1].replace(")","")

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS 
    object_name='LOCATION_DIMENSION_VALIDATE'
    action_type='UPDATE'
    action_func_total_counter=counter
    action_log_data = [action_id, object_name, action_type, start_time, update_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()

    # select details for current run in Action Log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    action_log_data = [action_id]
    cursor.execute(action_log_select_record,action_log_data)
    results = cursor.fetchall()
    print("AL   : Action_Log                     : Total Record Count           : ",results)

###########################
##### MAIN EXECUTION ######
###########################

def main():
    """
     Main Function to perform extract, transform all data in JSON format from song and user activity logs and load it into tables defined in PostgreSQL DB: sparkifydb
     Usage: Web_Extractor.py
    """
    options = Options()
    options.headless = True
    options.add_argument("no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=800,600")
    options.add_argument("--disable-dev-shm-usage")

    DRIVER_PATH = '/usr/bin/chromedriver'

    func_parameter=sys.argv[1]
    print(func_parameter)

    cursor, connection= create_database()
    
    if func_parameter=='validate_location_dimension':    
       validate_location_dimension(cursor,connection)
    elif func_parameter=='validate_job_dimension':    
       validate_job_dimension(cursor,connection,options,DRIVER_PATH)
    else:
       print("Not defined Parameter")

    connection.close()

if __name__ == "__main__":
    main()

