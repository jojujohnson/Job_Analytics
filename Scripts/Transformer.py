import os
import sys
import re
import glob
import shutil
import psycopg2
import pandas as pd
import time
import configparser
from datetime import date, datetime, timedelta
from db_objects_operations import *

config = configparser.ConfigParser()
config.read_file(open('/home/ubuntu/JobAnalytics/Config/config.cfg'))

#Get connection and key values from config file

PSQL_DB          = config.get("POSTGRESQL","PSQL_DB")
PSQL_DB_USER     = config.get("POSTGRESQL","PSQL_DB_USER")
PSQL_DB_PASSWORD = config.get("POSTGRESQL","PSQL_DB_PASSWORD")
PSQL_PORT        = config.get("POSTGRESQL","PSQL_PORT")
PSQL_HOSTNAME    = config.get("POSTGRESQL","PSQL_HOSTNAME")


####################################################################################################################################
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


####################################################################################################################################
def load_fact_function(cursor,connection):
    """
        Parameters:
            cursor (psycopg2.cursor()): DB Connection Cursor of JobAnalytics database
            connection
    """
    # select max of action_id + 1 and current db date time from action_log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    cursor.execute(action_log_select_max_action_id_plus_1_current_date_time)
    results = cursor.fetchall()
    print(results[0])
    split_data=re.split(",",str(results[0]))
    action_id=split_data[0].replace("(","")
    split_data=re.split(",",str(results[0]))
    start_time=split_data[1].replace(")","")

    cursor.execute(web_extractor_staging_select_status_2)
    results = cursor.fetchall()

    counter=0
    for record in results:
        print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~")
        company_name=record[0]
        job_title=str(record[1])
        job_assigned_id=str(record[2])
        job_location=str(record[3])
        job_raw_data=record[4]

        #select company_id from company_dimension ~ db_objects.py ~ SELECT TABLE OPERATIONS ~ REF : Multiple
        company_dimension_data = [company_name]
        cursor.execute(company_dimension_select_company_id,company_dimension_data)
        results = cursor.fetchall()
        Result=str(results[0])
        split_data=re.split(",",Result)
        company_id=split_data[0].replace("(","")

        #select job_id from job_dimension ~ db_objects.py ~ SELECT TABLE OPERATIONS
        job_dimension_data = [job_title]
        cursor.execute(job_dimension_select_job_id,job_dimension_data)
        results = cursor.fetchall()
        Result=str(results[0])
        split_data=re.split(",",Result)
        job_id=split_data[0].replace("(","")

        #select job_data_id from job_dimension ~ db_objects.py ~ SELECT TABLE OPERATIONS
        job_data_dimension_data = [job_assigned_id]
        cursor.execute(job_data_dimension_select_job_data_id,job_data_dimension_data)
        results = cursor.fetchall()
        Result=str(results[0])
        split_data=re.split(",",Result)
        job_data_id=split_data[0].replace("(","")

        #select location_id from location_dimension
        location_dimension_data = [job_location]
        cursor.execute(location_dimension_select_location_id,location_dimension_data)
        results = cursor.fetchall()
        Result=str(results[0])
        split_data=re.split(",",Result)
        location_id=split_data[0].replace("(","")

        print (company_name,"<-!->",job_title,"<-!->",job_assigned_id,"<-!->",job_location,"<-!->",job_raw_data)
        print (company_id,"<-!->",job_id,"<-!->",job_data_id,"<-!->",location_id)
 
        #insert company_job_location_fact
        record_status=1
        company_job_location_fact_data = [company_id, job_id, job_data_id, location_id, record_status]
        cursor.execute(company_job_location_fact_table_insert, company_job_location_fact_data)
        connection.commit()

        #update web_extractor_staging 
        extract_status=3
        web_extractor_staging_data = [extract_status, job_location, company_name, job_title, job_raw_data]
        cursor.execute(web_extractor_staging_table_update, web_extractor_staging_data)
        connection.commit()
        print (company_name,"<-!->",job_title,"<-!->",job_assigned_id,"<-!->",job_location,"<-!->",job_raw_data)
        print (company_id,"<-!->",job_id,"<-!->",location_id,"<-!->")
        counter+=1

    # select total record count from company_job_location_fact time >= start_time ~ db_objects.py ~ SELECT TABLE OPERATIONS
    filter_data = [start_time,start_time]
    cursor.execute(company_job_location_fact_select_create_update_count_gteq_start_time,filter_data)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    create_record_count=split_data[0].replace("(","")
    update_record_count=split_data[1].replace(")","")

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS 
    object_name='COMPANY_JOB_LOCATION_FACT'
    action_type='INSERT'
    action_func_total_counter=counter
    action_log_data = [action_id, object_name, action_type, start_time, create_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()

    # select details for current run in Action Log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    action_log_data = [action_id]
    cursor.execute(action_log_select_record,action_log_data)
    results = cursor.fetchall()
    print("AL   : Action_Log                  : Total Record Count           : ",results)

    cursor.execute(location_dimension_table_update_counter)
    connection.commit()

    cursor.execute(job_dimension_table_update_counter)
    connection.commit()

    cursor.execute(job_data_dimension_table_update_counter)
    connection.commit()

####################################################################################################################################
def load_location_job_dimension_function(cursor,connection):
    """
        Parameters:
            cursor (psycopg2.cursor()): DB Connection Cursor of JobAnalytics database
            connection
    """
    # select max of action_id + 1 and current db date time from action_log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    cursor.execute(action_log_select_max_action_id_plus_1_current_date_time)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    action_id=split_data[0].replace("(","")
    split_data=re.split(",",str(results[0]))
    start_time=split_data[1].replace(")","")

    # select records with status 1 from web_extractor_staging ~ db_objects.py ~ SELECT TABLE OPERATIONS
    cursor.execute(web_extractor_staging_select_status_1)
    results = cursor.fetchall()

    # select exclude keywords from location_dimension_exclude_keyword ~ db_objects.py ~ SELECT TABLE OPERATIONS ~ REF: Multiple
    cursor.execute(location_dimension_exclude_keyword_select)
    results_exclude_keyword = cursor.fetchall()

    counter=0
    data=[]

    for record in results:
        print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("company_name : ",record[0])
        company_name=record[0]
        job_title_extracted=str(record[1])
        job_assigned_id=record[2]
        PostDate=record[3]
        job_url=record[4]
        job_trimmed_data=record[5]
        job_raw_data=record[6]

        if PostDate:
           PostDate=PostDate.replace("+", "")
           PostDateSplit=PostDate.split()
           if PostDateSplit[0]=='Today':
              PostDays=0
           elif PostDateSplit[0]=='Yesterday':
              PostDays=1
           else:
              Search_keyword_today=re.search("Today",PostDate)
              Search_keyword_yesterday=re.search("Yesterday",PostDate)
              if Search_keyword_today:
                 PostDays=0
              elif Search_keyword_yesterday:
                 PostDays=1
              else:  
                 PostDays=int(PostDateSplit[0].strip())      

        Location=job_raw_data
        Location=Location.replace(job_assigned_id, "")
        Location=re.split("Posted",Location)
        Location=Location[0].strip()
        split_data=re.split("\|",Location)
        
        for i in range(0,len(split_data)):
            Location_Data=split_data[i].strip()
            if len(Location_Data)>0 :
             Location=Location_Data
             for record_exclude_keyword in results_exclude_keyword:
                ex_split_data=re.split("'",str(record_exclude_keyword))
                replace_keyword=ex_split_data[1]
                Location=Location.replace(replace_keyword, " ")
                Location=Location.replace("  ", " ")

        Location=Location.strip()
        if Location:
           location_extracted=Location
           location_actual='Validation_Pending'
        else:
           location_extracted='No Location'
           location_actual='No Location'
        location_raw=job_raw_data

        print("<-- Location Extracted-:",location_extracted,"-->")
        print("<-- Location Actual-:",location_actual,"-->")

        location_validation_status='Validation_Pending'

        # select max of location_id + 1 from location_dimension ~ db_objects.py ~ SELECT TABLE OPERATIONS
        cursor.execute(location_dimension_select_max_location_id_plus_1)
        results = cursor.fetchall()
        split_data=re.split(",",str(results[0]))
        location_id=split_data[0].replace("(","")

        # select max of job_id + 1 from job_dimension ~ db_objects.py ~ SELECT TABLE OPERATIONS
        cursor.execute(job_dimension_select_max_job_id_plus_1)
        results = cursor.fetchall()
        split_data=re.split(",",str(results[0]))
        job_id=split_data[0].replace("(","")

        # select max of job_id + 1 from job_dimension ~ db_objects.py ~ SELECT TABLE OPERATIONS
        cursor.execute(job_data_dimension_select_max_job_data_id_plus_1)
        results = cursor.fetchall()
        split_data=re.split(",",str(results[0]))
        job_data_id=split_data[0].replace("(","")

        #insert location_dimension ~ db_objects.py ~ INSERT TABLES
        location_update_counter=0
        location_dimension_data = [location_id, location_extracted, location_actual, location_validation_status, location_update_counter, location_update_counter, location_extracted]
        cursor.execute(location_dimension_table_insert, location_dimension_data)
        connection.commit()

        job_postdate=PostDays
        job_postdate=date.today() - timedelta(job_postdate)
        job_url_render_status='Render_Pending'
        job_validation_status='Validation_Pending'

        print("<-- Job ASSINGED ID -: ",job_assigned_id,"-->")
        print("<-- Job Title-: ",job_title_extracted,"-->")
        print("<-- Job POSTDATE-: ",job_postdate,"-->")
        print("<-- Job URL-: ",job_url,"-->")

        #insert job_dimension ~ db_objects.py ~ INSERT TABLES
        job_status=1
        job_update_counter=0
        job_dimension_data = [job_id, job_title_extracted, job_status, job_update_counter, job_update_counter, job_title_extracted]
        cursor.execute(job_dimension_table_insert, job_dimension_data)
        connection.commit() 

        #select job_id from job_dimension ~ db_objects.py ~ SELECT TABLE OPERATIONS
        job_dimension_data = [job_title_extracted]
        cursor.execute(job_dimension_select_id_with_title,job_dimension_data)
        results = cursor.fetchall()
        split_data=re.split(",",str(results[0]))
        job_id=split_data[0].replace("(","")

        #insert job_data_dimension ~ db_objects.py ~ INSERT TABLES
        job_type='Full_Time'
        job_data_update_counter=0
        print(job_data_id, job_id, job_assigned_id, job_url, job_type, job_postdate, job_url_render_status, job_validation_status, job_data_update_counter, job_id)
        job_data_dimension_data = [job_data_id, job_id, job_assigned_id, job_url, job_type, job_postdate, job_url_render_status, job_validation_status, job_data_update_counter, job_data_update_counter, job_id]
        cursor.execute(job_data_dimension_table_insert, job_data_dimension_data)
        connection.commit()

        #update web_extractor_staging ~ db_objects.py ~ UPDATE TABLES
        job_location=location_extracted
        extract_status=2
        #print (extract_status,"<-!->",job_location,"<-!->",company_name,"<-!->",job_title_extracted,"<-!->",job_raw_data,"<-!->",job_trimmed_data)
        web_extractor_staging_data = [extract_status, job_location, company_name, job_title_extracted, job_raw_data]
        cursor.execute(web_extractor_staging_table_update, web_extractor_staging_data)
        connection.commit()

        counter+=1

    action_func_total_counter=counter

    # select total record count from location_dimension | db_objects.py | REPORTING  OPERATIONS
    cursor.execute(location_dimension_select_total_count)
    results = cursor.fetchall()
    print("LD   : Location_Dimension                    : Total Record Count : ",results)

    # select total count by valid status from location_dimension | db_objects.py | REPORTING  OPERATIONS
    cursor.execute(location_dimension_select_count_gpby_vldstatus)
    results = cursor.fetchall()
    print("LD   : Location_Dimension                    : Validation Status  : ",results)

    # select total record count from job_dimension | db_objects.py | REPORTING  OPERATIONS
    cursor.execute(job_dimension_select_total_count)
    results = cursor.fetchall()
    print("\nJD  : Job_Dimension      : Total Record Count : ",results)

    # select total record count from job_data_dimension | db_objects.py | REPORTING  OPERATIONS
    cursor.execute(job_data_dimension_select_total_count)
    results = cursor.fetchall()
    print("JDD : Job_Data_Dimension : Total Record Count : ",results)
   
    # select total count by render status from job_data_dimension | db_objects.py | REPORTING  OPERATIONS
    cursor.execute(job_data_dimension_select_total_count_gpby_render_status)
    results = cursor.fetchall()
    print("JDD : Job_Data_Dimension : Render Status      : ",results)

    # select total count by validation status from job_data_dimension | db_objects.py | REPORTING  OPERATIONS
    cursor.execute(job_data_dimension_select_total_count_gpby_vld_status)
    results = cursor.fetchall()
    print("JDD : Job_Data_Dimension : Validation Status  : ",results)


    # select total record count from location_dimension time >= start_time ~ db_objects.py ~ SELECT TABLE OPERATIONS ~ REF: Multiple
    filter_data = [start_time,start_time]
    cursor.execute(location_dimension_select_create_update_count_gteq_start_time,filter_data)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    create_record_count=split_data[0].replace("(","")
    update_record_count=split_data[1].replace(")","")

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS 
    object_name='LOCATION_DIMENSION'
    action_type='INSERT'
    action_log_data = [action_id, object_name, action_type, start_time, create_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS
    object_name='LOCATION_DIMENSION'
    action_type='UPDATE'
    action_log_data = [action_id, object_name, action_type, start_time, update_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()


    # select total record count from job_dimension time >= start_time ~ db_objects.py ~ SELECT TABLE OPERATIONS
    filter_data = [start_time,start_time]
    cursor.execute(job_dimension_select_create_update_count_gteq_start_time,filter_data)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    create_record_count=split_data[0].replace("(","")
    update_record_count=split_data[1].replace(")","")

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS 
    object_name='JOB_DIMENSION'
    action_type='INSERT'
    action_log_data = [action_id, object_name, action_type, start_time, create_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS
    object_name='JOB_DIMENSION'
    action_type='UPDATE'
    action_log_data = [action_id, object_name, action_type, start_time, update_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()


    # select total record count from job_data_dimension time >= start_time ~ db_objects.py ~ SELECT TABLE OPERATIONS
    filter_data = [start_time,start_time]
    cursor.execute(job_data_dimension_select_create_update_count_gteq_start_time,filter_data)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    create_record_count=split_data[0].replace("(","")
    update_record_count=split_data[1].replace(")","")

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS 
    object_name='JOB_DATA_DIMENSION'
    action_type='INSERT'
    action_log_data = [action_id, object_name, action_type, start_time, create_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS
    object_name='JOB_DATA_DIMENSION'
    action_type='UPDATE'
    action_log_data = [action_id, object_name, action_type, start_time, update_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()


    # select details for current run in Action Log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    action_log_data = [action_id]
    cursor.execute(action_log_select_record,action_log_data)
    results = cursor.fetchall()
    print("AL  : Action_Log         : Total Record Count : ",results)


####################################################################################################################################
def load_web_extractor_staging_function(cursor,connection,project_path):
    """
        Reads Full_WebData_Source_Extract.txt.
        Parameters:
            cursor (psycopg2.cursor()): DB Connection Cursor of JobAnalytics database
            connection
            filepath (str): Filepath of the file to be processed
    """
    # select max of action_id + 1 and current db date time from action_log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    cursor.execute(action_log_select_max_action_id_plus_1_current_date_time)
    results = cursor.fetchall()
    print(results[0])
    split_data=re.split(",",str(results[0]))
    action_id=split_data[0].replace("(","")
    split_data=re.split(",",str(results[0]))
    start_time=split_data[1].replace(")","")

    Src_Ext_filepath=project_path+'/Source_Extractor/Full_WebData_Source_Extract.txt'
    Src_Ext_Err_filepath=project_path+'/Source_Extractor/Full_WebData_Source_Extract_Error.txt'
    Src_Ext_Exp_Err_filepath=project_path+'/Processing/EXTRACTION_EXCEPTION_FOUND.txt'

    # get current date and time
    currendatetime= datetime.now() 
    Date_Time_Str = currendatetime.strftime("%d-%m-%Y_%H:%M:%S")
    print("Date and Time =", Date_Time_Str)
    
    flag=0
    ext_counter=0
    ext_err_counter=0 

    if os.path.exists(Src_Ext_filepath): 
       print("\nFile Exists :", Src_Ext_filepath)   
    else:
       print("\nFile Not Found :", Src_Ext_filepath)
       flag=1

    #Check for Extract File Size and Processing
    if os.stat(Src_Ext_filepath).st_size > 0:
       #open and read file
       dataframe =  pd.read_csv(Src_Ext_filepath, sep='~', engine='python', header=None)

       extract_status=1
       for value in dataframe.values:
           company_name, job_title_extracted, job_assigned_id, job_posted, job_url, job_trimmed_data, job_raw_data = value
           print("<-- ",company_name," -- ",job_title_extracted," -- ",job_assigned_id," -- ",job_posted," -- ",job_url," -- ",job_trimmed_data," -- ",job_raw_data," -- ",extract_status," -->")

           #insert into web_extractor_staging ~ db_objects.py ~ INSERT TABLE
           web_extractor_staging_data = [company_name, job_title_extracted, job_assigned_id, job_posted, job_url, job_trimmed_data, job_raw_data, extract_status]
           cursor.execute(web_extractor_staging_table_insert, web_extractor_staging_data)
           connection.commit()
           ext_counter+=1
    else:
       print("No Records Zero Byte Extract File Found :", Src_Ext_filepath)
       exit()

    #
    source = Src_Ext_filepath
    destination=Src_Ext_filepath.replace("/Source_Extractor/","/Archive/")
    destination=destination.replace("Extract.txt","Extract_"+Date_Time_Str+".txt")
    if os.path.exists(source): 
       print("File Exists :", source)   
       perm = os.stat(source).st_mode 
       print("File Permission mode:", perm) 
       dest = shutil.copy(source, destination) 
       print("File Moved to Archive :", destination, "\n")
    else:
       print("File Web Extract Does Not Exist :", source)

    #
    source = Src_Ext_Exp_Err_filepath
    destination=Src_Ext_Exp_Err_filepath.replace("/Processing/","/Archive/")
    destination=destination.replace("EXTRACTION_EXCEPTION_FOUND.txt","EXTRACTION_EXCEPTION_FOUND_"+Date_Time_Str+".txt")
    if os.path.exists(source): 
       print("File Exists :", source)   
       perm = os.stat(source).st_mode 
       print("File Permission mode:", perm) 
       dest = shutil.move(source, destination) 
       print("File Moved to Archive :", destination, "\n")
    else:
       print("File Web Extract Does Not Exist :", source)

    #
    source=project_path+'/Processing/'
    destination=project_path+'/Archive/'
    files = os.listdir(source) 
    for file in files:
       print(source+''+file)
       check_source=source+''+file
       if os.path.exists(check_source):
        print("File Exists :", check_source)   
        perm = os.stat(check_source).st_mode 
        print("File Permission mode:", perm) 
  
        dest = shutil.move(check_source, destination) 
        print("File Moved to Archive :", destination, "\n")
       else:
        print("File Web Extracts Does Not Exist :", source)

    flag=0
    #Check for Error File
    if os.path.exists(Src_Ext_Err_filepath): 
       print("Error File Exists :", Src_Ext_Err_filepath)   
    else:
       print("Error File Not Found :", Src_Ext_Err_filepath)
       flag=1

    #Check for Error File Size and Processing
    if flag==0 and os.stat(Src_Ext_Err_filepath).st_size > 0:
       dataframe =  pd.read_csv(Src_Ext_Err_filepath, sep='~', engine='python', header=None)

       for value in dataframe.values:
           filler1, company_name, error_log , filler2, filler3 = value
           print("<-- ",company_name," -- ",error_log," -->")

           #insert web_extractor_staging_error ~ db_objects.py ~ INSERT TABLE
           web_extractor_error_staging_data = [company_name, error_log]
           cursor.execute(web_extractor_error_staging_table_insert, web_extractor_error_staging_data)
           connection.commit()
           ext_err_counter+=1

       source = Src_Ext_Err_filepath
       destination=Src_Ext_Err_filepath.replace("/Source_Extractor/","/Archive/")
       destination=destination.replace("Extract_Error.txt","Extract_Error_"+Date_Time_Str+".txt")
       if os.path.exists(source): 
          print("File Exists :", source)   
          perm = os.stat(source).st_mode 
          print("File Permission mode:", perm) 
  
          dest = shutil.copy(source, destination) 
          print("File Moved to Archive :", destination, "\n")
       else:
          print("File Does Not Exists :", source)
    else:
       print("No Records Zero Byte Error File Found :", source)

    # select distinct total record count from web_extractor_staging ~ db_objects.py ~ REPORTING  OPERATIONS 
    cursor.execute(web_extractor_staging_select_count_dist_compname)
    results = cursor.fetchall()
    print("\nWES  : Web_Extractor_Staging       : Distinct Company Name Count  : ",results)

    # select distinct total record count from web_extractor_error_staging ~ db_objects.py ~ REPORTING  OPERATIONS 
    cursor.execute(web_extractor_error_staging_select_count_dist_compname)
    results = cursor.fetchall()
    print("WEES : Web_Extractor_Error_Staging : Distinct Company Name Count  : ",results)

    # select total record count from web_extractor_staging ~ db_objects.py ~ REPORTING  OPERATIONS
    cursor.execute(web_extractor_staging_select_count)
    results = cursor.fetchall()
    print("WES  : Web_Extractor_Staging       : Total Record Count           : ",results)

    # select total record count by status from web_extractor_staging ~ db_objects.py ~ REPORTING  OPERATIONS
    cursor.execute(web_extractor_staging_select_count_gpby_status)
    results = cursor.fetchall()
    print("WES  : Web_Extractor_Staging       : Total Record Count by Status : ",results)

    # select total record count from web_extractor_staging time >= start_time ~ db_objects.py ~ SELECT TABLE OPERATIONS
    filter_data = [start_time]
    cursor.execute(web_extractor_staging_select_create_count_gteq_start_time,filter_data)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    create_record_count=split_data[0].replace("(","")
    
    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS 
    object_name='WEB_EXTRACT_STAGING'
    action_type='INSERT'
    action_func_total_counter=ext_counter
    action_log_data = [action_id, object_name, action_type, start_time, create_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()


    # select total record count from web_extractor_error_staging time >= start_time ~ db_objects.py ~ SELECT TABLE OPERATIONS
    filter_data = [start_time]
    cursor.execute(web_extractor_error_staging_select_create_count_gteq_start_time,filter_data)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    create_record_count=split_data[0].replace("(","")
    
    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS 
    object_name='WEB_EXTRACT_ERROR_STAGING'
    action_type='INSERT'
    action_func_total_counter=ext_err_counter
    action_log_data = [action_id, object_name, action_type, start_time, create_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()


    # select details for current run in Action Log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    action_log_data = [action_id]
    cursor.execute(action_log_select_record,action_log_data)
    results = cursor.fetchall()
    print("AL   : Action_Log                  : Total Record Count           : ",results)


####################################################################################################################################
def load_company_dimension_function(cursor,connection,filepath):
    """
        Reads Company_Master_Data.txt.
        Parameters:
            cursor (psycopg2.cursor()): DB Connection Cursor of JobAnalytics database
            connection
            filepath (str): Filepath of the file to be processed
    """

    # select max of action_id + 1 and current db date time from action_log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    cursor.execute(action_log_select_max_action_id_plus_1_current_date_time)
    results = cursor.fetchall()
    print(results[0])
    split_data=re.split(",",str(results[0]))
    action_id=split_data[0].replace("(","")
    split_data=re.split(",",str(results[0]))
    start_time=split_data[1].replace(")","")

    #open and read file
    dataframe =  pd.read_csv(filepath, sep='|', engine='python', header=None)

    counter=0
    for value in dataframe.values:
        company_name, career_url, career_framework = value
        print("<-- ",company_name," -- ",career_url," -- ",career_framework," -->")

        # select max of company_id + 1 from company_dimension ~ db_objects.py ~ SELECT TABLE OPERATIONS
        cursor.execute(company_dimension_select_max_company_id_plus_1)
        results = cursor.fetchall()
        split_data=re.split(",",str(results[0]))
        company_id=split_data[0].replace("(","")
        
        # insert company_id, company_name into company_dimension ~ db_objects.py ~ INSERT TABLE OPERATIONS
        company_dimension_data = [company_id, company_name]
        cursor.execute(company_dimension_table_insert, company_dimension_data)
        connection.commit()

        # select company_id from company_dimension ~ db_objects.py ~ SELECT TABLE OPERATIONS
        company_dimension_data = [company_name]
        cursor.execute(company_dimension_select_company_id,company_dimension_data)
        results = cursor.fetchall()
        split_data=re.split(",",str(results[0]))
        company_id=split_data[0].replace("(","")

        # select max of company_job_url_id + 1 from company_job_url_dimension ~ db_objects.py ~ SELECT TABLE OPERATIONS
        cursor.execute(company_job_url_dimension_select_max_company_id_plus_1)
        results = cursor.fetchall()
        split_data=re.split(",",str(results[0]))
        company_job_url_id=split_data[0].replace("(","")

        # insert company_job_url_id, company_id, career_url, career_framework into company_job_url_dimension ~ db_objects.py ~ INSERT TABLE OPERATIONS 
        company_job_url_dimension_data = [company_job_url_id, company_id, career_url, career_framework]
        cursor.execute(company_job_url_dimension_table_insert, company_job_url_dimension_data)
        connection.commit()

        counter+=1

    # select total record count from company_dimension time >= start_time ~ db_objects.py ~ SELECT TABLE OPERATIONS
    filter_data = [start_time,start_time]
    cursor.execute(company_dimension_select_create_update_count_gteq_start_time,filter_data)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    create_record_count=split_data[0].replace("(","")
    update_record_count=split_data[1].replace(")","")

    action_func_total_counter=counter

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS 
    object_name='COMPANY_DIMENSION'
    action_type='INSERT'
    action_log_data = [action_id, object_name, action_type, start_time, create_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS
    object_name='COMPANY_DIMENSION'
    action_type='UPDATE'
    action_log_data = [action_id, object_name, action_type, start_time, update_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()

    # select total record count from company_dimension time >= start_time ~ db_objects.py ~ SELECT TABLE OPERATIONS
    filter_data = [start_time,start_time]
    cursor.execute(company_job_url_dimension_select_count_gteq_start_time,filter_data)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    create_record_count=split_data[0].replace("(","")
    update_record_count=split_data[1].replace(")","")

    action_func_total_counter=counter

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS
    object_name='COMPANY_JOB_URL_DIMENSION'
    action_type='INSERT'
    action_log_data = [action_id, object_name, action_type, start_time, create_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS
    object_name='COMPANY_JOB_URL_DIMENSION'
    action_type='UPDATE'
    action_log_data = [action_id, object_name, action_type, start_time, update_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()


    # select total expected and actual record count from company_dimension_count_select_expected_actual table ~ db_objects.py ~ SELECT TABLE OPERATIONS
    cursor.execute(company_dimension_select_total_count)
    results = cursor.fetchall()
    print("\nCD   : Company_Dimension           : Total Record Count           : ",results)

    # select total expected and actual job url record count from company_job_url_dimension_count_select_expected_actual table ~ db_objects.py ~ SELECT TABLE OPERATIONS
    cursor.execute(company_job_url_dimension_select_total_count_expected_actual)
    results = cursor.fetchall()
    print("CD   : Company_Job_URL_Dimension   : Total Record Count           : ",results)

    # select details for current run in Action Log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    action_log_data = [action_id]
    cursor.execute(action_log_select_record,action_log_data)
    results = cursor.fetchall()
    print("AL   : Action_Log                  : Total Record Count           : ",results)


####################################################################################################################################
def load_location_dimension_exclude_keyword_function(cursor,connection,filepath):
    """
        Parameters:
            cursor (psycopg2.cursor()): DB Connection Cursor of JobAnalytics database
            connection
    """
    # select max of action_id + 1 and current db date time from action_log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    cursor.execute(action_log_select_max_action_id_plus_1_current_date_time)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    action_id=split_data[0].replace("(","")
    split_data=re.split(",",str(results[0]))
    start_time=split_data[1].replace(")","")

    #open and read file
    dataframe =  pd.read_csv(filepath, engine='python', sep='|', header=None, encoding='utf-8')
    
    counter=0
    for value in dataframe.values:
        id, exclude_keyword = value
        print("<-- ",exclude_keyword," -->")

        #insert location_dimension_exclude_keyword | db_objects.py | INSERT TABLES
        location_dimension_exclude_keyword_data = [exclude_keyword]
        cursor.execute(location_dimension_exclude_keyword_table_insert, location_dimension_exclude_keyword_data)
        connection.commit()
        counter+=1

    # select total count from location_dimension_exclude_keyword ~ db_objects.py ~ REPORTING OPERATIONS
    cursor.execute(location_dimension_exclude_keyword_select_total_count)
    results = cursor.fetchall()
    print("\nLDEK : Location_Dimension_Exclude_Keyword    : Total Record Count : ",results)

    # select total record count from location_dimension_exclude_keyword time >= start_time ~ db_objects.py ~ SELECT TABLE OPERATIONS
    filter_data = [start_time,start_time]
    cursor.execute(location_dimension_exclude_keyword_select_create_update_count_gteq_start_time,filter_data)
    results = cursor.fetchall()
    split_data=re.split(",",str(results[0]))
    create_record_count=split_data[0].replace("(","")
    update_record_count=split_data[1].replace(")","")

    action_func_total_counter=counter

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS
    object_name='LOCATION_DIMENSION_EXCLUDE_KEYWORD'
    action_type='INSERT'
    action_log_data = [action_id, object_name, action_type, start_time, create_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()

    # insert action_id, object_name, action_type, action_total_records into action_log ~ db_objects.py ~ INSERT TABLE OPERATIONS
    object_name='LOCATION_DIMENSION_EXCLUDE_KEYWORD'
    action_type='UPDATE'
    action_log_data = [action_id, object_name, action_type, start_time, update_record_count, action_func_total_counter]
    cursor.execute(action_log_table_insert, action_log_data)
    connection.commit()

    # select details for current run in Action Log ~ db_objects.py ~ SELECT TABLE OPERATIONS
    action_log_data = [action_id]
    cursor.execute(action_log_select_record,action_log_data)
    results = cursor.fetchall()
    print("AL   : Action_Log                            : Record Details     : ",results)


####################################################################################################################################
def select_db_details_function(cursor,connection):
    """
        Parameters:
            cursor (psycopg2.cursor()): DB Connection Cursor of JobAnalytics database
            connection
    """
    # db_objects | Reporting Query LIST
    cursor.execute(web_extractor_staging_select_count_gpby_compname)
    results = cursor.fetchall()
    print("WES    : Web_Extractor_Staging   : Distinct Company Name Count     : ",results)

    #
    cursor.execute(company_dimension_select_top_one_record_per_company_name)
    results = cursor.fetchall()
    print("\nCD     : Company_Dimension       : Top_One_Record_Per_Company_Name : ",results)

    #
    cursor.execute(missing_company_name_btw_cd_wes)
    results = cursor.fetchall()
    print("\nCD WES : Missing_Company_Name_Between_Company_Dimension_&_Web_Extract_Staging : ",results)

    #
    cursor.execute(company_dimension_select_total_count)
    results = cursor.fetchall()
    print("\nCD   : Company_Dimension           : Total Record Count               : ",results)

    #
    cursor.execute(company_job_url_dimension_select_total_count_expected_actual)
    results = cursor.fetchall()
    print("CD   : Company_Job_URL_Dimension   : Total Record Count               : ",results)
 
    #
    cursor.execute(web_extractor_staging_select_count_dist_compname)
    results = cursor.fetchall()
    print("\nWES  : Web_Extractor_Staging       : Distinct Company Name Count      : ",results)

    #
    cursor.execute(web_extractor_error_staging_select_count_dist_compname)
    results = cursor.fetchall()
    print("WEES : Web_Extractor_Error_Staging : Distinct Company Name Count      : ",results)

    #
    cursor.execute(web_extractor_staging_select_count)
    results = cursor.fetchall()
    print("WES  : Web_Extractor_Staging       : Total Record Count               : ",results)

    #
    cursor.execute(web_extractor_staging_select_count_gpby_status)
    results = cursor.fetchall()
    print("WES  : Web_Extractor_Staging       : Total Record Count by Status     : ",results)

    #
    cursor.execute(web_extractor_staging_select_distinct_count)
    results = cursor.fetchall()
    print("WES  : Web_Extractor_Staging       : Distinct Total Record Count      : ",results)

    #
    cursor.execute(web_extractor_staging_select_distinct_count_gpby_status)
    results = cursor.fetchall()
    print("WES  : Web_Extractor_Staging       : Distinct Record Count by Status  : ",results)

    #
    cursor.execute(location_dimension_exclude_keyword_select_total_count)
    results = cursor.fetchall()
    print("\nLDEK : Location_Dimension_Exclude_Keyword    : Total Record Count     : ",results)

    #
    cursor.execute(location_dimension_select_total_count)
    results = cursor.fetchall()
    print("LD   : Location_Dimension                    : Total Record Count     : ",results)

    #
    cursor.execute(location_dimension_select_count_gpby_vldstatus)
    results = cursor.fetchall()
    print("LD   : Location_Dimension                    : Validation Status      : ",results)

    #
    cursor.execute(job_dimension_select_total_count)
    results = cursor.fetchall()
    print("\nJD   : Job_Dimension                         : Total Record Count     : ",results)

    #
    cursor.execute(job_data_dimension_select_total_count)
    results = cursor.fetchall()
    print("JDD  : Job_Data_Dimension                    : Total Record Count     : ",results)
   
    #
    cursor.execute(job_data_dimension_select_total_count_gpby_render_status)
    results = cursor.fetchall()
    print("JDD  : Job_Data_Dimension                    : Render Status          : ",results)

    #
    cursor.execute(job_data_dimension_select_total_count_gpby_vld_status)
    results = cursor.fetchall()
    print("JDD  : Job_Data_Dimension                    : Validation Status      : ",results)

    #
    cursor.execute(company_job_location_select_fact_count)
    results = cursor.fetchall()
    print("\nCJLF : Company_Job_Location_Fact             : Total Record Count     : ",results)

    #
    cursor.execute(staging_dimension_fact_select_common_dist_record_count)
    results = cursor.fetchall()
    print("\nAll            : Staging_Dimension_Fact        : Total Record Count             : ",results)

    #
    cursor.execute(dimension_fact_select_job_url_available_count)
    results = cursor.fetchall()
    print("Dimension_Fact : Dimension_Fact                : JOB URL Available Record Count : ",results)

    #
    cursor.execute(dimension_table_select_counter_data)
    results = cursor.fetchall()
    print("LJD            : Location_Job_Dimension  Union : Total Counter Records          : \n",results)

    #
    cursor.execute(action_log_select_count)
    results = cursor.fetchall()
    print("\nAL             : Action_Log                    : Total Record Count             : ",results)

    #
    cursor.execute(action_log_select)
    results = cursor.fetchall()
    results=str(results)
    results=results.replace("[(","\n [(")
    results=results.replace(", (","\n, (")
    results=results.replace(",","|")
    results=results.replace("datetime.datetime(","")
    results=results.replace(")","")
    print("AL             : Action_Log                    : Action Log Summary             : ",results)

####################################################################################################################################
def db_backup_function(cursor,connection):
    """
        Parameters:
            cursor (psycopg2.cursor()): DB Connection Cursor of JobAnalytics database
            connection
    """
    #cursor.execute(web_extractor_staging_db_backup)
    #chmod 7777 ./JobAnalytics/Backup/    
    #sudo -u postgres psql -f ./JobAnalytics/Scripts/jobanalytics_backup.sql
    #chmod 0775 ./JobAnalytics/Backup/ 
    print("DB Backup Completed")

####################################################################################################################################
def db_restore_function(cursor,connection):
    """
        Parameters:
            cursor (psycopg2.cursor()): DB Connection Cursor of JobAnalytics database
            connection
    """
    cursor.execute(web_extractor_staging_db_restore)
    print("DB Restore Completed")


####################################################################################################################################
def main():
    """
        Main Function
        Usage: python3 db_operations.py
    """

    func_parameter=sys.argv[1]
    print(func_parameter)

    cursor, connection= create_database()

    print('Home Path : ' + os.environ['HOME'])
    homepath=os.environ['HOME']
    print('Working Directory : ' + homepath)

    config = configparser.ConfigParser()
    config.read(homepath +'/JobAnalytics/Config/config.cfg')
    os.environ['PROJECT'] = config.get('PROJECT_ANALYTICS', 'PROJECT')

    print('Project : ' + os.environ['PROJECT'])

    project_path=homepath+'/'+os.environ['PROJECT']
    print('Project Path : ' + project_path + '\n')
    
    if func_parameter=='load_company_dimension':    
       load_company_dimension_function(cursor , connection , filepath=project_path+'/Reference_Data/Company_Dimension_Master_Data.txt')
    elif func_parameter=='load_dimension_exclude_keyword':    
       load_location_dimension_exclude_keyword_function(cursor , connection , filepath=project_path+'/Reference_Data/Location_Dimension_Exclude_Keyword_Data.txt')
    elif func_parameter=='load_web_extractor_staging':    
       load_web_extractor_staging_function(cursor , connection , project_path)
    elif func_parameter=='load_location_job_dimension':    
       load_location_job_dimension_function(cursor , connection)
    elif func_parameter=='load_fact':    
       load_fact_function(cursor , connection)
    elif func_parameter=='summary':    
       select_db_details_function(cursor , connection)
    elif func_parameter=='db_backup':    
       db_backup_function(cursor , connection)
    elif func_parameter=='db_restore':    
       db_restore_function(cursor , connection)
    else:
       print("Not defined Parameter")

    connection.close()

if __name__ == "__main__":
    main()