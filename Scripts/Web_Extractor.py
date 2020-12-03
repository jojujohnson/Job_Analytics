import os
import re
import time
import shutil
import configparser
import psycopg2
import urllib.request
from datetime import datetime
from db_objects_operations import *
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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


def workday(source_company,source_url,source_framework,fulldatafile,fulldataerrorfile,driver):
    print('Executing WorkDay Function')

    currendatetime= datetime.now() 
    Date_Time_Str = currendatetime.strftime("%d-%m-%Y_%H:%M:%S")
    print("date and time =", Date_Time_Str)
 
    try: 
     driver.get(source_url)

     last_height = driver.execute_script("return document.body.scrollHeight")
     first_height=last_height
     new_height=-1

     while last_height != new_height:

        last_height = new_height
   	# Scroll down to the bottom.
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

   	# Wait to load the page.
        time.sleep(2)

   	# Calculate new scroll height and compare with last scroll height.
        new_height = driver.execute_script("return document.body.scrollHeight")
        print(last_height,new_height)

     soup = BeautifulSoup(driver.page_source, 'lxml')
     #print (soup.prettify())
   
     if first_height == last_height:
        print("No Scroll Data")
        #print("<-- COMPANY -: ~"+source_company+"~ !!! Data Rendering Failed !!! ~"+source_framework+"~ -->")
        #filerec="<-- COMPANY -: ~"+source_company+"~ !!! Data Rendering Failed !!! ~"+source_framework+"~ -->"
        #fulldataerrorfile.write(str(filerec))
        #fulldataerrorfile.write("\n") 

     if soup:
        print ("\\\ Data Available For Processing ///") 

        # datetime object containing current date and time

        req = urllib.request.Request(source_url)
        req.add_header('Accept', 'application/json,application/xml')
        file=urllib.request.urlopen(req).read().decode('utf-8')
        filedata=file.replace('[','\n')
        filedata=filedata.replace('(','~')
        filedata=filedata.replace(')','~')

        src_filename=source_company+"_career_url_decoded_temp_data_"+Date_Time_Str+".txt"
        src_filename_retain=src_filename
        src_file=open(src_filename, "w")
        src_file.write(filedata)
        src_file.close()

        search_url_data=open(src_filename_retain)

        src_filename=source_company+"_career_url_temp_data_"+Date_Time_Str+".txt"
        src_file=open(src_filename, "w")
        src_file.write(soup.get_text(separator="\n"))
        src_file.close()

        search_text_tags=open(src_filename)
        
        if os.path.exists(src_filename):
           print("The file found for deletion :", src_filename)
           #os.remove(src_filename)
        else:
           print("The file does not exist for deletion")

        for dataline in search_text_tags:
          dataline=dataline.strip()
          if len(dataline) > 0:
            keywordsearch=re.search("Posted", dataline)
            if '|' in dataline and keywordsearch: 
               print("#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#\n")
               job_title=prevrecord
               job_title=job_title.strip()
               dataline=dataline.strip()
               job_raw_data=str(dataline)
               job_url='JOB_URL_DATA_NOT_AVAILABLE'
               job_trimmed_data=''
               job_assigned_id='JOB_ID_NOT_AVAILABLE'

               print(dataline)
               try:
               #if dataline:
                   job_title_temp=job_title.replace('(','~')
                   job_title_temp=job_title_temp.replace(')','~')

                   split_data=re.split("\|",dataline)
                   for x in range(len(split_data)):
                       arr_data=split_data[x]
                       arr_data=arr_data.strip()
                       arr_data=arr_data.replace('-','CHARSPECIALHYPHEN')
                       arr_data=arr_data.strip()
                       arr_data=re.subn('\W', ' ', arr_data)
                       arr_data=arr_data[0]
                       arr_data=arr_data.replace('CHARSPECIALHYPHEN','-')
                       #arr_data=arr_data.replace("\n","")
                       Search_PostDate=re.search("Posted",arr_data)
                       Search_Space=len(re.findall('\s', arr_data))

                       if Search_PostDate:
                          Search_PostDate=re.split("Posted",arr_data)
                          job_posted=Search_PostDate[1]
                       elif Search_Space or arr_data.isalpha():
                          print("in1 -->",arr_data)
                          arr_data=re.sub('\n', '', arr_data)
                          job_trimmed_data=job_trimmed_data+''+arr_data+'|'
                       else:             
                          print("in3 -->",arr_data)
                          Search_JobId1=re.sub(r"[^a-zA-Z0-9]+", ' ', arr_data)
                          Search_JobId1=re.search("[0-9]*[A-Z]*\W?[0-9]*\W?[0-9]*", Search_JobId1)
                          if Search_JobId1:
                             job_assigned_id=arr_data
                             
                   search_url_data=open(src_filename_retain)
                   for record_line in search_url_data:
                       if re.search(job_title_temp, record_line):
                          if re.search(job_assigned_id, record_line):
                             #print(record_line)
                             split_data=re.split('"',record_line)
                             #print(split_data[21])
                             split_data=re.split('/job/',split_data[21])
                             #print(source_url+'/job/'+split_data[1])
                             job_url=source_url+'/job/'+split_data[1]

                   print("<-- COMPANY -: ",source_company,"-->")
                   print("<-- JOB ASSIGNED ID -: ",job_assigned_id,"-->")
                   print("<-- PROCESSING RECORD -: ",job_title+" ~ "+job_assigned_id+" ~ "+job_posted+" ~ "+job_url+" ~ "+job_trimmed_data+" ~ "+job_raw_data+" ~ "+source_url,"-->")
                   print("<-- JOB URL -: ",job_url,"-->")

                   filerec=str(source_company+'~'+job_title+'~'+job_assigned_id+'~'+job_posted+'~'+job_url+'~'+job_trimmed_data+'~'+job_raw_data)
                   fulldatafile.write(str(filerec))
                   fulldatafile.write("\n")
                
               except:
                   print("EXTRACTION_EXCEPTION_FOUND")
                   print("<-- EXCEPTION RECORD -: ",source_company+" ~ "+job_title+" ~ "+job_assigned_id+" ~ "+job_posted+" ~ "+job_url+" ~ "+job_trimmed_data+" ~ "+job_raw_data+" ~ "+source_url,"-->")
                   filerec=str(source_company+'~'+job_title+'~'+job_assigned_id+'~'+job_posted+'~'+job_url+'~'+job_trimmed_data+'~'+job_raw_data)  
                   extraction_err_file.write(filerec)
                   extraction_err_file.write("\n")
               
            prevrecord=dataline
       
    except: 
      print("<-- COMPANY -: ~"+source_company+"~ !!! Extraction Exception2 Found !!! ~"+source_framework+"~ -->")
      filerec="<-- COMPANY -: ~"+source_company+"~ !!! Extraction Exception2 Found !!! ~"+source_framework+"~ -->"
      fulldataerrorfile.write(str(filerec))
      fulldataerrorfile.write("\n") 
      driver.quit()  



def workday_retry(source_company,source_url,source_framework,fulldatafile,fulldataerrorfile,driver):
    print('Executing WorkDay Function')

    currendatetime= datetime.now() 
    Date_Time_Str = currendatetime.strftime("%d-%m-%Y_%H:%M:%S")
    print("date and time =", Date_Time_Str)
 
    try: 
     driver.get(source_url)

     last_height = driver.execute_script("return document.body.scrollHeight")
     first_height=last_height
     new_height=-1

     while last_height != new_height:

        last_height = new_height
   	# Scroll down to the bottom.
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

   	# Wait to load the page.
        time.sleep(2)

   	# Calculate new scroll height and compare with last scroll height.
        new_height = driver.execute_script("return document.body.scrollHeight")
        print(last_height,new_height)

     soup = BeautifulSoup(driver.page_source, 'lxml')
     #print (soup.prettify())
   
     if first_height == last_height:
        print("No Scroll Data")
        #print("<-- COMPANY -: ~"+source_company+"~ !!! Data Rendering Failed !!! ~"+source_framework+"~ -->")
        #filerec="<-- COMPANY -: ~"+source_company+"~ !!! Data Rendering Failed !!! ~"+source_framework+"~ -->"
        #fulldataerrorfile.write(str(filerec))
        #fulldataerrorfile.write("\n") 

     if soup:
        print ("\\\ Data Available For Processing ///") 

        # datetime object containing current date and time

        req = urllib.request.Request(source_url)
        req.add_header('Accept', 'application/json,application/xml')
        file=urllib.request.urlopen(req).read().decode('utf-8')
        filedata=file.replace('[','\n')
        filedata=filedata.replace('(','~')
        filedata=filedata.replace(')','~')

        src_filename=source_company+"_career_url_decoded_temp_data_"+Date_Time_Str+".txt"
        src_filename_retain=src_filename
        src_file=open(src_filename, "w")
        src_file.write(filedata)
        src_file.close()

        search_url_data=open(src_filename_retain)

        src_filename=source_company+"_career_url_temp_data_"+Date_Time_Str+".txt"
        src_file=open(src_filename, "w")
        src_file.write(soup.get_text(separator="\n"))
        src_file.close()

        search_text_tags=open(src_filename)
        
        if os.path.exists(src_filename):
           print("The file found for deletion :", src_filename)
           #os.remove(src_filename)
        else:
           print("The file does not exist for deletion")

        for dataline in search_text_tags:
          dataline=dataline.strip()
          if len(dataline) > 0:
            keywordsearch=re.search("Posted", dataline)
            if '|' in dataline and keywordsearch: 
               print("#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#*#\n")
               job_title=prevrecord
               job_title=job_title.strip()
               dataline=dataline.strip()
               job_raw_data=str(dataline)
               job_url='JOB_URL_DATA_NOT_AVAILABLE'
               job_trimmed_data=''
               job_assigned_id='JOB_ID_NOT_AVAILABLE'

               print(dataline)
               try:
               #if dataline:
                   job_title_temp=job_title.replace('(','~')
                   job_title_temp=job_title_temp.replace(')','~')

                   split_data=re.split("\|",dataline)
                   for x in range(len(split_data)):
                       arr_data=split_data[x]
                       arr_data=arr_data.strip()
                       arr_data=arr_data.replace('-','CHARSPECIALHYPHEN')
                       arr_data=arr_data.strip()
                       arr_data=re.subn('\W', ' ', arr_data)
                       arr_data=arr_data[0]
                       arr_data=arr_data.replace('CHARSPECIALHYPHEN','-')
                       #arr_data=arr_data.replace("\n","")
                       Search_PostDate=re.search("Posted",arr_data)
                       Search_Space=len(re.findall('\s', arr_data))

                       if Search_PostDate:
                          Search_PostDate=re.split("Posted",arr_data)
                          job_posted=Search_PostDate[1]
                       elif Search_Space or arr_data.isalpha():
                          print("in1 -->",arr_data)
                          arr_data=re.sub('\n', '', arr_data)
                          job_trimmed_data=job_trimmed_data+''+arr_data+'|'
                       else:             
                          print("in3 -->",arr_data)
                          Search_JobId1=re.sub(r"[^a-zA-Z0-9]+", ' ', arr_data)
                          Search_JobId1=re.search("[0-9]*[A-Z]*\W?[0-9]*\W?[0-9]*", Search_JobId1)
                          if Search_JobId1:
                             job_assigned_id=arr_data
                             
                   search_url_data=open(src_filename_retain)
                   for record_line in search_url_data:
                       if re.search(job_title_temp, record_line):
                          if re.search(job_assigned_id, record_line):
                             #print(record_line)
                             split_data=re.split('"',record_line)
                             #print(split_data[21])
                             split_data=re.split('/job/',split_data[21])
                             #print(source_url+'/job/'+split_data[1])
                             job_url=source_url+'/job/'+split_data[1]

                   print("<-- COMPANY -: ",source_company,"-->")
                   print("<-- JOB ASSIGNED ID -: ",job_assigned_id,"-->")
                   print("<-- PROCESSING RECORD -: ",job_title+" ~ "+job_assigned_id+" ~ "+job_posted+" ~ "+job_url+" ~ "+job_trimmed_data+" ~ "+job_raw_data+" ~ "+source_url,"-->")
                   print("<-- JOB URL -: ",job_url,"-->")

                   filerec=str(source_company+'~'+job_title+'~'+job_assigned_id+'~'+job_posted+'~'+job_url+'~'+job_trimmed_data+'~'+job_raw_data)
                   fulldatafile.write(str(filerec))
                   fulldatafile.write("\n")
                
               except:
                   print("EXTRACTION_EXCEPTION_FOUND")
                   print("<-- EXCEPTION RECORD -: ",source_company+" ~ "+job_title+" ~ "+job_assigned_id+" ~ "+job_posted+" ~ "+job_url+" ~ "+job_trimmed_data+" ~ "+job_raw_data+" ~ "+source_url,"-->")
                   filerec=str(source_company+'~'+job_title+'~'+job_assigned_id+'~'+job_posted+'~'+job_url+'~'+job_trimmed_data+'~'+job_raw_data)  
                   extraction_err_file.write(filerec)
                   extraction_err_file.write("\n")
               
            prevrecord=dataline
       
    except: 
      print("<-- COMPANY -: ~"+source_company+"~ !!! Extraction Exception2 Found !!! ~"+source_framework+"~ -->")
      filerec="<-- COMPANY -: ~"+source_company+"~ !!! Extraction Exception2 Found !!! ~"+source_framework+"~ -->"
      fulldataerrorfile.write(str(filerec))
      fulldataerrorfile.write("\n") 
      driver.quit()  


def inhouse(source_company,source_url,source_framework):
     print('Executing InHouse Function') 
     exit()
    #try:     
     #time.sleep(5)
     #state=[]
     #state=
     #driver.find_element_by_xpath("/html/body/ats-app/div/app-page-layout/section/main/app-search/section/div/multi-input-search/div/div/div[2]/ifc-taleo-search-results/ifc-taleo-search-results-template/div/div/button").send_keys('.')
     #print(state)

     #state=WebDriverWait(driver,10).until(EC.element_to_be_clickable((By.XPATH,"/html/body/ats-app/div/app-page-layout/section/main/app-search/section/div/multi-input-search/div/div/div[2]/ifc-taleo-search-results/ifc-taleo-search-results-template/div/div/button")))
     #print(state)

     #for i in range (0,len(state)-1):
     #  print(i)
       #state[i].click()
     
     driver.get(source_url)

     last_height = driver.execute_script("return document.body.scrollHeight")
     print(last_height)
     first_height=last_height
     new_height=-1

     while last_height != new_height:

        last_height = new_height
   	# Scroll down to the bottom.
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

   	# Wait to load the page.
        time.sleep(2)

   	# Calculate new scroll height and compare with last scroll height.
        new_height = driver.execute_script("return document.body.scrollHeight")
        print(last_height,new_height)

     soup = BeautifulSoup(driver.page_source, "html5lib")
     #print (soup.prettify())
     driver.quit()
   
     if first_height == last_height:
        print('!!! Data Rendering Failed !!!')
        filerec="<-- COMPANY -: ",source_company," !!! Data Rendering Failed !!! -->"
        fulldatafile.write(str(filerec))
        fulldatafile.write("\n")
        src_filename=source_company+"_temp.txt"
        src_file=open(src_filename, "w")
        src_file.write(soup.prettify())
        #src_file.write(soup.get_text(separator="\n"))
        src_file.close()
        	
     else:
        print ("\\\ Data Available For Processing ///") 
        
        src_filename=source_company+"_temp.txt"
        src_file=open(src_filename, "w")
        src_file.write(soup.prettify())
        #src_file.write(soup.get_text(separator="\n"))
        src_file.close()
   
        search_text_tags=open(src_filename)
        
        if os.path.exists(src_filename):
           print("The file found for deletion :", src_filename)
           #os.remove(src_filename)
        else:
           print("The file does not exist for deletion")

    #except: 	
     print("!!! Exception Found In",source_framework,"!!!")
     driver.quit()  


###########################
##### MAIN EXECUTION ######
###########################

def main():
    """
        Main Function to perform extract, transform all data in JSON format from song and user activity logs and load it into tables defined in PostgreSQL DB: sparkifydb
        Usage: Web_Extractor.py
    """

    print('Home Path : ' + os.environ['HOME'])
    homepath=os.environ['HOME']
    print('Working Directory : ' + homepath)

    config = configparser.ConfigParser()
    config.read(homepath +'/JobAnalytics/Config/config.cfg')
    os.environ['PROJECT'] = config.get('PROJECT_ANALYTICS', 'PROJECT')

    print('Project : ' + os.environ['PROJECT'])

    project_path=homepath+'/'+os.environ['PROJECT']
    print('Project Path : ' + project_path + '\n')

    if os.path.exists(project_path): 
       print("Directory Exists :", project_path)   
    else:
       os.mkdir(create_path)
       print("Directory Created :", project_path)

    os.chdir(project_path)
    Working_Dir=os.getcwd()
    print('Changing Current Working Directory : ' + Working_Dir + '\n')

    create_path='./Reference_Data'
    if os.path.exists(create_path): 
       print("Directory Exists :", create_path)   
    else:
       os.mkdir(create_path)
       print("Directory Created :", create_path)

    create_path='./Scripts'
    if os.path.exists(create_path): 
       print("Directory Exists :", create_path)   
    else:
       os.mkdir(create_path)
       print("Directory Created :", create_path)

    create_path='./Source_Extractor'
    if os.path.exists(create_path): 
       print("Directory Exists :", create_path)   
    else:
       os.mkdir(create_path)
       print("Directory Created :", create_path)

    create_path='./Processing'
    if os.path.exists(create_path): 
       print("Directory Exists :", create_path)   
    else:
       os.mkdir(create_path)
       print("Directory Created :", create_path)

    os.chdir(create_path)
    Working_Dir=os.getcwd()
    print('Changing Current Working Directory : ' + Working_Dir + '\n')
   
    file_data=[]

    fullfile="Full_WebData_Source_Extract.txt"
    fulldatafile=open(fullfile, "w")
    fullerrfile="Full_WebData_Source_Extract_Error.txt"
    fulldataerrorfile=open(fullerrfile, "w")

    extraction_err_file="EXTRACTION_EXCEPTION_FOUND.txt"
    extraction_err_file=open(extraction_err_file, "w")

    options = Options()
    options.headless = True
    options.add_argument("no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=800,600")
    options.add_argument("--disable-dev-shm-usage")

    DRIVER_PATH = '/usr/bin/chromedriver'

    cursor, connection= create_database()
    cursor.execute(company_dimension_select)
    results = cursor.fetchall()
    connection.close()

    for record in range(0,len(results)):
        print ("########################################")
        print("\nDB RECORD : ",results[record])
        driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)

        company=results[record][1]
        url=results[record][2]
        framework_type=results[record][3]

        company=company.strip()
        url=url.strip()
        framework_type=framework_type.strip()
        company=company.strip(' ')
        url=url.strip(' ')

        print ("\n<-- COMPANY -: ",company,"-->")
        print ("<-- URL -: ",url,"-->")
        print ("<-- FRAMEWORK -: ",framework_type,"-->")

        if framework_type=='workday':
           workday(company,url,framework_type,fulldatafile,fulldataerrorfile,driver)
        if framework_type=='workday_retry':
           workday(company,url,framework_type,fulldatafile,fulldataerrorfile,driver)
        else:
           print(company,url,framework_type)

        driver.quit()
	
    fulldatafile.close()
    fulldataerrorfile.close()
    extraction_err_file.close()

    source = str(project_path)+'/Processing/'+str(fullfile)
    destination = str(project_path)+'/Source_Extractor/'+str(fullfile)

    if os.path.exists(source): 
       print("File Exists :", source)   
       perm = os.stat(source).st_mode 
       print("File Permission mode:", perm, "\n") 
  
       dest = shutil.move(source, destination) 
    else:
       print("File Does Not Exists :", source)

    source = str(project_path)+'/Processing/'+str(fullerrfile)
    destination = str(project_path)+'/Source_Extractor/'+str(fullerrfile)

    if os.path.exists(source): 
       print("File Exists :", source)   
       perm = os.stat(source).st_mode 
       print("File Permission mode:", perm, "\n") 
  
       dest = shutil.move(source, destination) 
    else:
       print("File Does Not Exists :", source)
  
if __name__ == "__main__":
    main()
