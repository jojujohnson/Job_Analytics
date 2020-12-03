#########################################################################################################################################
#########################################################################################################################################
#                                                       DROP TABLE OPERATIONS                                                           #
#########################################################################################################################################
company_dimension_table_drop = "DROP TABLE IF EXISTS company_dimension"
company_job_url_dimension_table_drop = "DROP TABLE IF EXISTS company_job_url_dimension"

share_price_dimension_table_drop = "DROP TABLE IF EXISTS share_price_dimension"
news_dimension_table_drop = "DROP TABLE IF EXISTS news_dimension_dimension"
rank_index_dimension_table_drop = "DROP TABLE IF EXISTS rank_index_dimension"
service_product_dimension_table_drop = "DROP TABLE IF EXISTS service_product_dimension"
revenue_dimension_table_drop = "DROP TABLE IF EXISTS revenue_dimension"
hr_dimension_table_drop = "DROP TABLE IF EXISTS hr_dimension"

web_extractor_staging_table_drop = "DROP TABLE IF EXISTS web_extractor_staging"
web_extractor_error_staging_table_drop = "DROP TABLE IF EXISTS web_extractor_error_staging"
web_extractor_hist_staging_table_drop = "DROP TABLE IF EXISTS web_extractor_hist_staging"
web_extractor_hist_error_staging_table_drop = "DROP TABLE IF EXISTS web_extractor_hist_error_staging"

location_dimension_exclude_keyword_table_drop= "DROP TABLE IF EXISTS location_dimension_exclude_keyword"
location_dimension_table_drop = "DROP TABLE IF EXISTS location_dimension"

job_dimension_table_drop = "DROP TABLE IF EXISTS job_dimension"
job_data_dimension_table_drop = "DROP TABLE IF EXISTS job_data_dimension"
skill_dimension_table_drop = "DROP TABLE IF EXISTS skill_dimension"

company_job_location_fact_table_drop = "DROP TABLE IF EXISTS company_job_location_fact"
action_log_table_drop = "DROP TABLE IF EXISTS action_log"


#########################################################################################################################################
#########################################################################################################################################
#                                                CREATE DIMENSION TABLE OPERATIONS                                                      #
#########################################################################################################################################
company_dimension_table_create = ("""
    CREATE TABLE IF NOT EXISTS company_dimension
    (company_id int PRIMARY KEY, 
     company_name varchar, 
     company_type varchar, 
     company_HQ varchar, 
     company_url varchar, 
     wikipedia_url varchar,
     linkedin_url varchar,
     glassdoor_url varchar,
     share_keyword varchar,
     company_status int,
     create_date TIMESTAMP,
     update_date TIMESTAMP,
     unique(company_name))
""")

company_job_url_dimension_table_create = ("""
    CREATE TABLE IF NOT EXISTS company_job_url_dimension
    (company_job_url_id int PRIMARY KEY,
     company_id int, 
     career_url varchar,
     career_framework varchar,
     create_date TIMESTAMP,
     update_date TIMESTAMP,
     unique(company_id, career_url))
""")


share_price_dimension_table_create = ("""
    CREATE TABLE IF NOT EXISTS share_price_dimension 
    (share_price_id int PRIMARY KEY,
     company_id int,
     share_price float,
     year int,  
     timeframe_start TIMESTAMP,
     timeframe_end TIMESTAMP,
     create_date TIMESTAMP, 
     update_date TIMESTAMP
)
""")

news_dimension_table_create = ("""
    CREATE TABLE IF NOT EXISTS news_dimension
    (news_id int PRIMARY KEY,
     company_id int,
     news_head_line varchar,
     news_url varchar,
     create_date TIMESTAMP, 
     update_date TIMESTAMP)
""")

rank_index_dimension_table_create = ("""
    CREATE TABLE IF NOT EXISTS rank_index_dimension
    (rank_index_id int PRIMARY KEY,
     company_id int,
     type varchar,
     forbes_rank varchar,
     year int,  
     timeframe_start TIMESTAMP,
     timeframe_end TIMESTAMP,
     create_date TIMESTAMP, 
     update_date TIMESTAMP
)
""")

service_product_dimension_table_create = ("""
    CREATE TABLE IF NOT EXISTS service_product_dimension
    (service_product_id int PRIMARY KEY,
     company_id int,
     sp_name varchar,
     sp_url varchar,
     type varchar,
     year int,  
     timeframe_start TIMESTAMP,
     timeframe_end TIMESTAMP,
     create_date TIMESTAMP, 
     update_date TIMESTAMP)
""")

revenue_dimension_table_create = ("""
    CREATE TABLE IF NOT EXISTS revenue_dimension
    (revenue_id int PRIMARY KEY,
     company_id int,
     revenue varchar,
     year int,  
     timeframe_start TIMESTAMP,
     timeframe_end TIMESTAMP,
     create_date TIMESTAMP, 
     update_date TIMESTAMP
)
""")
 
hr_dimension_table_create = ("""
    CREATE TABLE IF NOT EXISTS hr_dimension 
    (hr_id int PRIMARY KEY,
     company_id int,
     employee_count float,
     year int,  
     timeframe_start TIMESTAMP,
     timeframe_end TIMESTAMP,
     create_date TIMESTAMP, 
     update_date TIMESTAMP
)
""")


web_extractor_staging_table_create = ("""
    CREATE TABLE IF NOT EXISTS web_extractor_staging
    (company_name varchar,
     job_title varchar,
     job_assigned_id varchar,
     job_posted varchar,
     job_location varchar,
     job_url varchar,
     job_trimmed_data varchar,
     job_raw_data varchar,
     extract_status int,
     create_date TIMESTAMP, 
     update_date TIMESTAMP)
""")

web_extractor_hist_staging_table_create = ("""
    CREATE TABLE IF NOT EXISTS web_extractor_hist_staging
    (company_name varchar,
     job_title varchar,
     job_assigned_id varchar,
     job_posted varchar,
     job_location varchar,
     job_url varchar,
     job_trimmed_data varchar,
     job_raw_data varchar,
     extract_status int,
     create_date TIMESTAMP, 
     update_date TIMESTAMP)
""")

web_extractor_error_staging_table_create = ("""
    CREATE TABLE IF NOT EXISTS web_extractor_error_staging
    (company_name varchar,
     error_log varchar,
     create_date TIMESTAMP, 
     update_date TIMESTAMP)
""")

web_extractor_hist_error_staging_table_create = ("""
    CREATE TABLE IF NOT EXISTS web_extractor_hist_error_staging
    (company_name varchar,
     error_log varchar,
     create_date TIMESTAMP, 
     update_date TIMESTAMP)
""")


location_dimension_table_create = ("""
    CREATE TABLE IF NOT EXISTS location_dimension 
    (location_id int PRIMARY KEY,
     location_extracted varchar,
     location_actual varchar,
     latitude float, 
     longitude float,
     location_array varchar,
     location_array_hit_keyword varchar,
     location_arr_hit_position varchar,
     location_reverse_value varchar,
     gmap_link varchar, 
     timezone varchar, 
     timenow varchar,  
     location_validation_status varchar,
     location_status int,
     location_update_counter int,  
     location_update_counter_total int,  
     create_date TIMESTAMP, 
     update_date TIMESTAMP,
     unique(location_extracted))
""")

location_dimension_exclude_keyword_table_create = ("""
    CREATE TABLE IF NOT EXISTS location_dimension_exclude_keyword 
    (exclude_keyword varchar,
     create_date TIMESTAMP, 
     update_date TIMESTAMP,
     unique(exclude_keyword))
""")


job_dimension_table_create = ("""
    CREATE TABLE IF NOT EXISTS job_dimension 
    (job_id int PRIMARY KEY,
     job_title_extracted varchar,
     job_title_actual varchar,
     job_status int,
     job_update_counter int, 
     job_update_counter_total int, 
     create_date TIMESTAMP, 
     update_date TIMESTAMP,
     unique(job_title_extracted))
""")

job_data_dimension_table_create = ("""
    CREATE TABLE IF NOT EXISTS job_data_dimension 
    (job_data_id int PRIMARY KEY,
     job_id int,
     job_assigned_id varchar,
     job_url varchar,
     job_type varchar,
     job_department varchar,
     job_postdate varchar, 
     job_data varchar, 
     job_url_render_status varchar,
     job_validation_status varchar, 
     job_data_update_counter int, 
     job_data_update_counter_total int, 
     create_date TIMESTAMP, 
     update_date TIMESTAMP,
     unique(job_id,job_assigned_id))
""")

skill_dimension_table_create = ("""
    CREATE TABLE IF NOT EXISTS skill_dimension
    (skill_id varchar PRIMARY KEY,
     skill_name varchar, 
     create_date TIMESTAMP, 
     update_date TIMESTAMP)
""")


#########################################################################################################################################
#########################################################################################################################################
#                                                  CREATE FACT TABLE OPERATIONS                                                         #
#########################################################################################################################################
company_job_location_fact_table_create = ("""
    CREATE TABLE IF NOT EXISTS company_job_location_fact
    (fact_id int,
     company_id int, 
     job_id int, 
     job_data_id int,
     location_id int,
     record_status int,  
     skill_id int,
     create_date TIMESTAMP, 
     update_date TIMESTAMP,
     unique(company_id, job_id, job_data_id, location_id))
""")

action_log_table_create = ("""
    CREATE TABLE IF NOT EXISTS action_log
    (action_id int,
     object_name varchar, 
     action_type varchar, 
     start_time TIMESTAMP,
     end_time TIMESTAMP, 
     action_db_total_records int,
     action_func_total_counter int,     
     create_date TIMESTAMP, 
     update_date TIMESTAMP,
     unique(object_name, action_type, start_time, end_time))
""")


#########################################################################################################################################
#########################################################################################################################################
#                                                      INSERT TABLE OPERATIONS                                                          #
#########################################################################################################################################
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - load_company_dimension_function (2) |
action_log_table_insert = ("""
    INSERT INTO action_log
    (action_id, object_name, action_type, start_time, action_db_total_records, action_func_total_counter, end_time, create_date)
    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) 
    ON CONFLICT DO NOTHING;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py | load_company_dimension_function
company_dimension_table_insert = ("""
    INSERT INTO company_dimension
    (company_id, company_name, create_date)
    VALUES (%s, %s, CURRENT_TIMESTAMP) 
    ON CONFLICT (company_name) DO UPDATE SET update_date = CURRENT_TIMESTAMP;
""")

# db_operations.py | load_company_dimension_function
company_job_url_dimension_table_insert = ("""
    INSERT INTO company_job_url_dimension
    (company_job_url_id, company_id, career_url, career_framework, create_date)
    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP) 
    ON CONFLICT (company_id, career_url) DO UPDATE SET update_date = CURRENT_TIMESTAMP;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py | load_web_extractor_staging_function
web_extractor_staging_table_insert = ("""
    INSERT INTO web_extractor_staging
    (company_name, job_title, job_assigned_id, job_posted, job_url, job_trimmed_data, job_raw_data, extract_status, create_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT DO NOTHING;
""")

# db_operations.py | load_web_extractor_staging_function
web_extractor_error_staging_table_insert = ("""
    INSERT INTO web_extractor_error_staging
    (company_name, error_log, create_date)
    VALUES (%s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT DO NOTHING;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py | load_location_dimension_exclude_keyword_function
location_dimension_exclude_keyword_table_insert = ("""
    INSERT INTO location_dimension_exclude_keyword
    (exclude_keyword,create_date)
    VALUES (%s, CURRENT_TIMESTAMP)
    ON CONFLICT (exclude_keyword) DO UPDATE SET update_date = CURRENT_TIMESTAMP;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py | load_location_job_dimension_function 
location_dimension_table_insert = ("""
    INSERT INTO location_dimension
    (location_id, location_extracted, location_actual, location_validation_status, location_update_counter, location_update_counter_total, create_date)
    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (location_extracted) DO UPDATE SET update_date = CURRENT_TIMESTAMP, 
    location_update_counter=(select COALESCE(max(location_update_counter),0)+1 from location_dimension where location_extracted=%s);
""")

# db_operations.py | load_location_job_dimension_function
job_dimension_table_insert = ("""
    INSERT INTO job_dimension
    (job_id, job_title_extracted, job_status, job_update_counter, job_update_counter_total, create_date)
    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (job_title_extracted) DO UPDATE SET update_date = CURRENT_TIMESTAMP, job_status=1, 
    job_update_counter=(select COALESCE(max(job_update_counter),0)+1 from job_dimension where job_title_extracted=%s);
""")

# db_operations.py | load_location_job_dimension_function
job_data_dimension_table_insert = ("""
    INSERT INTO job_data_dimension
    (job_data_id, job_id, job_assigned_id, job_url, job_type, job_postdate, job_url_render_status, job_validation_status, job_data_update_counter, job_data_update_counter_total, create_date)
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (job_id,job_assigned_id) DO UPDATE SET update_date = CURRENT_TIMESTAMP, 
    job_data_update_counter=(select COALESCE(max(job_data_update_counter),0)+1 from job_data_dimension where job_id=%s);
""")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py | load_fact_function
company_job_location_fact_table_insert = ("""
    INSERT INTO company_job_location_fact
    (company_id, job_id, job_data_id, location_id, record_status, create_date)
    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (company_id, job_id, job_data_id, location_id) DO UPDATE SET update_date = CURRENT_TIMESTAMP;
""")


#########################################################################################################################################
#########################################################################################################################################
#                                                     UPDATE TABLE OPERATIONS                                                           #
#########################################################################################################################################
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py | load_location_job_dimension_function
web_extractor_staging_table_update = ("""
    UPDATE web_extractor_staging
    SET extract_status=%s,
    job_location=%s,
    update_date = CURRENT_TIMESTAMP
    where 
    company_name=%s and
    job_title=%s and
    job_raw_data=%s 
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
location_dimension_table_update = ("""
    UPDATE location_dimension
    SET location_actual=%s,
    latitude=%s,
    longitude=%s,
    location_array=%s,
    location_array_hit_keyword=%s,
    location_arr_hit_position=%s,
    location_reverse_value=%s,
    gmap_link=%s,
    timezone=%s,
    timenow=%s,
    location_validation_status=%s,
    location_status=%s,
    update_date=CURRENT_TIMESTAMP
    where location_id=%s;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
job_data_dimension_table_update = ("""
    UPDATE job_data_dimension
    SET job_type=%s,
    job_department=%s,
    job_data=%s,
    job_url_render_status=%s,
    job_validation_status=%s,
    update_date=CURRENT_TIMESTAMP
    where job_id=%s;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py | load_fact_function
location_dimension_table_update_counter = ("""
    UPDATE location_dimension
    SET location_update_counter_total=location_update_counter_total+location_update_counter where 1=1;
    UPDATE location_dimension
    SET location_update_counter=0;
""")

# db_operations.py | load_fact_function
job_dimension_table_update_counter = ("""
    UPDATE job_dimension
    SET job_update_counter_total=job_update_counter_total+job_update_counter where 1=1;
    UPDATE job_dimension
    SET job_update_counter=0;
""")

# db_operations.py | load_fact_function
job_data_dimension_table_update_counter = ("""
    UPDATE job_data_dimension
    SET job_data_update_counter_total=job_data_update_counter_total+job_data_update_counter where 1=1;
    UPDATE job_data_dimension
    SET job_data_update_counter=0;
""")
	
#########################################################################################################################################
#########################################################################################################################################
#                                                     SELECT TABLE OPERATIONS                                                           #
#########################################################################################################################################
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - load_company_dimension_function     | db_operations.py - load_location_dimension_exclude_keyword_function
# db_operations.py - load_web_extractor_staging_function | db_operations.py - load_fact_function
action_log_select_max_action_id_plus_1_current_date_time = ("""
    select COALESCE(max(action_id),0)+1,current_timestamp::varchar from action_log;
""")

# db_operations.py - load_company_dimension_function     | db_operations.py - load_location_dimension_exclude_keyword_function
# db_operations.py - load_web_extractor_staging_function | db_operations.py - load_fact_function
action_log_select_record = ("""
    select action_id,object_name,action_type,start_time::varchar,end_time::varchar,action_db_total_records,action_func_total_counter 
    from action_log where action_id=%s;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - load_company_dimension_function
company_dimension_select_max_company_id_plus_1 = ("""
    select COALESCE(max(company_id),0)+1 from company_dimension;
""")

# db_operations.py - load_company_dimension_function |  db_operations.py - load_fact_function
company_dimension_select_company_id = ("""
   select company_id from company_dimension where company_name=%s limit 1;
""")

# db_operations.py - load_company_dimension_function
company_job_url_dimension_select_max_company_id_plus_1 = ("""
    select COALESCE(max(company_job_url_id),0)+1 from company_job_url_dimension;
""")

# db_operations.py - load_company_dimension_function
company_dimension_select_create_update_count_gteq_start_time = ("""
    select t1.count, t2.count from
    (select count(*) from company_dimension where create_date>=%s) t1,
    (select count(*) from company_dimension where update_date>=%s) t2;
""")

# db_operations.py - load_company_dimension_function
company_job_url_dimension_select_count_gteq_start_time = ("""
    select t1.count, t2.count from
    (select count(*) from company_job_url_dimension where create_date>=%s) t1,
    (select count(*) from company_job_url_dimension where update_date>=%s) t2;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - location_dimension_exclude_keyword |  db_operations.py - load_location_job_dimension_function
location_dimension_exclude_keyword_select_create_update_count_gteq_start_time = ("""
    select t1.count, t2.count from
    (select count(*) from location_dimension_exclude_keyword where create_date>=%s) t1,
    (select count(*) from location_dimension_exclude_keyword where update_date>=%s) t2;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Web_Extractor.py - Main
company_dimension_select = ("""
    SELECT distinct cd.company_id, company_name, career_url, career_framework 
    from company_job_url_dimension cjud, company_dimension cd where cd.company_id=cjud.company_id order by cd.company_id asc;
""")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - web_extract_staging
web_extractor_staging_select_create_count_gteq_start_time = ("""
    select count(*) from web_extractor_staging where create_date>=%s;
""")

# db_operations.py - web_extract_error_staging
web_extractor_error_staging_select_create_count_gteq_start_time = ("""
    select count(*) from web_extractor_error_staging where create_date>=%s;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - load_location_job_dimension_function
web_extractor_staging_select_status_1 = ("""
    SELECT distinct company_name,job_title,job_assigned_id,job_posted,job_url,job_trimmed_data,job_raw_data from web_extractor_staging where extract_status=1 order by company_name,job_title,job_assigned_id,job_trimmed_data asc;
""")

# db_operations.py - load_location_job_dimension_function
location_dimension_exclude_keyword_select = ("""
    SELECT distinct exclude_keyword from location_dimension_exclude_keyword;
""")

# db_operations.py - load_location_job_dimension_function
location_dimension_select_max_location_id_plus_1 = ("""
    select COALESCE(max(location_id),0)+1 from location_dimension;
""")

# db_operations.py - load_location_job_dimension_function
job_dimension_select_max_job_id_plus_1 = ("""
    select COALESCE(max(job_id),0)+1 from job_dimension;
""")

# db_operations.py - load_location_job_dimension_function
job_data_dimension_select_max_job_data_id_plus_1 = ("""
    select COALESCE(max(job_data_id),0)+1 from job_data_dimension;
""")

# db_operations.py - load_location_job_dimension_function
job_dimension_select_id_with_title= ("""
    select job_id,job_title_extracted from job_dimension where job_title_extracted=%s;
""")

# db_operations.py - location_dimension_function
location_dimension_select_create_update_count_gteq_start_time = ("""
    select t1.count, t2.count from
    (select count(*) from location_dimension where create_date>=%s) t1,
    (select count(*) from location_dimension where update_date>=%s) t2;
""")

# db_operations.py - job_dimension_function
job_dimension_select_create_update_count_gteq_start_time = ("""
    select t1.count, t2.count from
    (select count(*) from job_dimension where create_date>=%s) t1,
    (select count(*) from job_dimension where update_date>=%s) t2;
""")

# db_operations.py - job_data_dimension_function
job_data_dimension_select_create_update_count_gteq_start_time = ("""
    select t1.count, t2.count from
    (select count(*) from job_data_dimension where create_date>=%s) t1,
    (select count(*) from job_data_dimension where update_date>=%s) t2;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - load_fact_function
web_extractor_staging_select_status_2 = ("""
    SELECT distinct company_name,job_title,job_assigned_id,job_location,job_raw_data 
    from web_extractor_staging where extract_status=2 order by company_name,job_title,job_raw_data asc;
""")

# company_dimension_select_company_id - Referenced

# db_operations.py - load_fact_function
job_dimension_select_job_id = ("""        
    select job_id from job_dimension where job_title_extracted=%s limit 1
""")

# db_operations.py - load_fact_function
job_data_dimension_select_job_data_id = ("""        
    select job_data_id from job_data_dimension where job_assigned_id=%s limit 1
""")

# db_operations.py - load_fact_function
location_dimension_select_location_id = ("""
   select location_id from location_dimension where location_extracted=%s limit 1;
""")

# db_operations.py - load_fact_function
company_job_location_fact_select_create_update_count_gteq_start_time = ("""
    select t1.count, t2.count from
    (select count(*) from company_job_location_fact where create_date>=%s) t1,
    (select count(*) from company_job_location_fact where update_date>=%s) t2;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
location_dimension_validation_select_validation_status_pending = ("""
    select location_id,location_extracted from location_dimension order by location_id asc;
""")

job_data_dimension_validation_select_validation_pending = ("""
    select job_id,job_url from job_data_dimension where job_validation_status='Validation_Pending' order by job_id asc;
""")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
web_extractor_staging_select_status_3 = ("""
    SELECT distinct company_name,job_title,job_raw_data from web_extractor_staging where extract_status=3 order by company_name,job_title,job_raw_data asc;
""")


#########################################################################################################################################
#########################################################################################################################################
#                                                       REPORTING  OPERATIONS                                                           #
#########################################################################################################################################
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - select_db_details_function
web_extractor_staging_select_count_gpby_compname = ("""
    select company_name,count(*) from web_extractor_staging group by company_name order by count(*) desc;
""")

# db_operations.py - select_db_details_function
company_dimension_select_top_one_record_per_company_name= ("""
    select company_name,job_assigned_id,length(job_assigned_id),job_raw_data from (
    SELECT company_name,job_assigned_id,job_title,job_raw_data,row_number() OVER(PARTITION BY company_name) rnum
    FROM  web_extractor_staging
    GROUP BY company_name,job_assigned_id,job_title,job_raw_data) as t1
    where t1.rnum=1 order by length(t1.job_assigned_id),company_name asc;
""")

# db_operations.py - select_db_details_function
missing_company_name_btw_cd_wes= ("""
    select distinct cd.company_name as company_name_cd from company_dimension cd 
    LEFT JOIN web_extractor_error_staging wes ON cd.company_name=wes.company_name where cd.company_name not in 
    (select company_name from web_extractor_staging) and wes.company_name is null;
""")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - select_db_details_function | db_operations.py - load_company_dimension_function
company_dimension_select_total_count = ("""
    SELECT 'Expected : '||count(distinct(company_name)), 'Actual : '||count(*) from company_dimension;
""")

# db_operations.py - select_db_details_function | db_operations.py - load_company_dimension_function
company_job_url_dimension_select_total_count_expected_actual = ("""
    SELECT 'Expected : '||count(distinct career_url), 'Actual : '||count(*) from company_job_url_dimension;
""")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - select_db_details_function | db_operations.py - load_web_extractor_staging_function
web_extractor_staging_select_count_dist_compname= ("""
    select count(distinct company_name) from web_extractor_staging;
""")

# db_operations.py - select_db_details_function | db_operations.py - load_web_extractor_staging_function
web_extractor_error_staging_select_count_dist_compname = ("""
    select count(distinct company_name) from web_extractor_error_staging;
""")

# db_operations.py - select_db_details_function | db_operations.py - load_web_extractor_staging_function
web_extractor_staging_select_count= ("""
    SELECT count(*) from web_extractor_staging;
""")

# db_operations.py - select_db_details_function | db_operations.py - load_web_extractor_staging_function
web_extractor_staging_select_count_gpby_status= ("""
    SELECT 'Extract Status : '||extract_status,count(*) t1_count from web_extractor_staging group by extract_status order by extract_status asc;
""")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - select_db_details_function | db_operations.py - load_web_extractor_staging_function
web_extractor_staging_select_distinct_count= ("""
    select count(distinct company_name::varchar || job_title::varchar || job_assigned_id::varchar || job_location::varchar)
    from  web_extractor_staging ;
""")

# db_operations.py - select_db_details_function | db_operations.py - load_web_extractor_staging_function
web_extractor_staging_select_distinct_count_gpby_status= ("""
    SELECT 'Extract Status : '||extract_status,
    count(distinct company_name::varchar || job_title::varchar || job_assigned_id::varchar || job_location::varchar) t1_count 
    from web_extractor_staging group by extract_status order by extract_status asc;
""")

# db_operations.py - select_db_details_function | db_operations.py - load_location_dimension_exclude_keyword_function
location_dimension_exclude_keyword_select_total_count= ("""
    SELECT count(*) from location_dimension_exclude_keyword;
""")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - select_db_details_function | db_operations.py - load_location_job_dimension_function
location_dimension_select_total_count = ("""
    SELECT count(*) from location_dimension;
""")

# db_operations.py - select_db_details_function | db_operations.py - load_location_job_dimension_function
location_dimension_select_count_gpby_vldstatus= ("""
    select location_validation_status,count(*) from location_dimension group by location_validation_status order by count(*) desc;
""")

# db_operations.py - select_db_details_function | db_operations.py - load_location_job_dimension_function
job_dimension_select_total_count = ("""
    SELECT count(*) from job_dimension;
""")

# db_operations.py - select_db_details_function | db_operations.py - load_location_job_dimension_function
job_data_dimension_select_total_count = ("""
    SELECT count(*) from job_data_dimension;
""")

# db_operations.py - select_db_details_function | db_operations.py - load_location_job_dimension_function
job_data_dimension_select_total_count_gpby_render_status = ("""
    SELECT job_url_render_status,count(*) from job_data_dimension group by job_url_render_status order by count(*) desc;
""")

# db_operations.py - select_db_details_function | db_operations.py - load_location_job_dimension_function
job_data_dimension_select_total_count_gpby_vld_status = ("""
    SELECT job_validation_status,count(*) from job_data_dimension group by job_validation_status order by count(*) desc;
""")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# db_operations.py - select_db_details_function
company_job_location_select_fact_count = ("""
    SELECT count(*) from company_job_location_fact;
""")

# db_operations.py - select_db_details_function
staging_dimension_fact_select_common_dist_record_count= ("""
    select count(distinct cd.company_id::varchar || jd.job_id::varchar || ld.location_id::varchar || jdd.job_data_id::varchar)
    from  company_job_location_fact cjlf , 
        company_dimension cd,
        location_dimension ld, 
        job_dimension jd, job_data_dimension jdd,
        web_extractor_staging wes
    where cjlf.company_id=cd.company_id and
        cjlf.job_id=jd.job_id and
        cjlf.job_data_id=jdd.job_data_id and
        cjlf.location_id=ld.location_id and
        wes.company_name=cd.company_name and
        wes.job_title=jd.job_title_extracted and
        wes.job_location=ld.location_extracted and 
        wes.job_assigned_id=jdd.job_assigned_id;
""")

# db_operations.py - select_db_details_function
dimension_fact_select_job_url_available_count = ("""
    select count(distinct cd.company_name || jd.job_title_extracted || ld.location_extracted || jdd.job_postdate || jdd.job_url || cjud.career_url)
    from  company_job_location_fact cjlf , 
        company_dimension cd, company_job_url_dimension cjud,
        location_dimension ld, 
        job_dimension jd, job_data_dimension jdd
    where cjlf.company_id=cd.company_id and
        cjlf.job_id=jd.job_id and
        cjlf.job_data_id=jdd.job_data_id and
        cjlf.location_id=ld.location_id and
        cd.company_id=cjud.company_id and 
        jdd.job_url like '%'||cjud.career_url||'%';
""")

# db_operations.py - select_db_details_function
dimension_table_select_counter_data = ("""
   (select 'Location : '||count(location_id) count, sum(location_update_counter) as c, sum(location_update_counter_total) as t from location_dimension) 
   union
   (select 'Job      : '||count(job_id) count, sum(job_update_counter) as c, sum(job_update_counter_total) as t  from job_dimension)
   union
   (select 'Job Data : '||count(job_data_id) count, sum(job_data_update_counter) as c, sum(job_data_update_counter_total) as t from job_data_dimension);
""")

# db_operations.py - select_db_details_function
action_log_select_count= ("""
    SELECT count(*) from action_log;
""")

# db_operations.py - select_db_details_function
action_log_select = ("""
    select action_id ,object_name, action_type, start_time::varchar start_time, end_time::varchar end_time, action_db_total_records db, action_func_total_counter file 
    from action_log where action_id >= (select max(action_id) from action_log where object_name='COMPANY_DIMENSION' and action_type='INSERT');
""")

#########################################################################################################################################
#########################################################################################################################################
#                                                     SELECT Query LIST : JOIN                                                          #
#########################################################################################################################################
company_job_dimension_join_select = ("""
    SELECT company_id, cd.company_name company_name, career_url, job_id, job_title, job_raw_data 
    from job_dimension jd INNER join company_dimension cd ON jd.company_name=cd.company_name;
""")


#########################################################################################################################################
#########################################################################################################################################
#                                                SEQUENCE CREATE and DROP OPERATIONS                                                    #
#########################################################################################################################################
company_dimension_sequence_create = ("""
CREATE SEQUENCE IF NOT EXISTS company_dimension_sequence
  start 1
  increment 1;
""")

job_dimension_sequence_create = ("""
CREATE SEQUENCE IF NOT EXISTS job_dimension_sequence
  start 1
  increment 1;
""")

job_data_dimension_sequence_create = ("""
CREATE SEQUENCE IF NOT EXISTS job_data_dimension_sequence
  start 1
  increment 1;
""")

location_dimension_sequence_create = ("""
CREATE SEQUENCE IF NOT EXISTS location_dimension_sequence
  start 1
  increment 1;
""")

company_dimension_sequence_drop = ("""
DROP SEQUENCE IF EXISTS company_dimension_sequence;
""")

job_dimension_sequence_drop = ("""
DROP SEQUENCE IF EXISTS job_dimension_sequence;
""")

job_data_dimension_sequence_drop = ("""
DROP SEQUENCE IF EXISTS job_data_dimension_sequence;
""")

location_dimension_sequence_drop = ("""
DROP SEQUENCE IF EXISTS location_dimension_sequence;
""")


#########################################################################################################################################
#########################################################################################################################################
#                                                   INDEX CREATE and DROP OPERATIONS                                                    #
#########################################################################################################################################
job_dimension_unquie_index_create = ("""
CREATE UNIQUE INDEX job_unq_ind ON job_dimension (job_title_extracted)
WHERE job_title_extracted IS NOT NULL;
""")

job_dimension_unquie_index_drop = ("""
DROP INDEX IF EXISTS job_unq_ind;
""")


#########################################################################################################################################
#########################################################################################################################################
#                                                     TRUNCATE and DELETE OPERATIONS                                                    #
#########################################################################################################################################
web_extractor_staging_truncate = ("""
    truncate table web_extractor_staging;
""")

web_extractor_error_staging_truncate = ("""
    truncate table web_extractor_error_staging;
""")

# db_objects.py - load_location_dimension_exclude_keyword_function
location_dimension_exclude_keyword_truncate = ("""
    truncate table location_dimension_exclude_keyword;
""")


#########################################################################################################################################
#########################################################################################################################################
#                                                       DB BACKUP and RESTORE                                                           #
#########################################################################################################################################
web_extractor_staging_db_backup = ("""
    COPY (SELECT * FROM web_extractor_staging) TO '/home/ubuntu/JobAnalytics/Backup/web_extractor_staging.csv';
""")

web_extractor_staging_db_restore = ("""
    COPY web_extractor_staging FROM '/home/ubuntu/JobAnalytics/Backup/web_extractor_staging.csv';
""")


#########################################################################################################################################
#########################################################################################################################################
#                                                         DB OPERATIONS                                                                 #
#########################################################################################################################################
drop_table_queries = [company_dimension_table_drop, company_job_url_dimension_table_drop, web_extractor_staging_table_drop, web_extractor_error_staging_table_drop, web_extractor_hist_staging_table_drop, web_extractor_hist_error_staging_table_drop, share_price_dimension_table_drop, news_dimension_table_drop, rank_index_dimension_table_drop, service_product_dimension_table_drop, revenue_dimension_table_drop, hr_dimension_table_drop, location_dimension_exclude_keyword_table_drop, location_dimension_table_drop, job_dimension_table_drop, job_data_dimension_table_drop, skill_dimension_table_drop, company_job_location_fact_table_drop, action_log_table_drop]
drop_sequence_queries = [company_dimension_sequence_drop, job_dimension_sequence_drop, job_data_dimension_sequence_drop, location_dimension_sequence_drop]
drop_index_queries = [job_dimension_unquie_index_drop]

create_table_queries = [company_dimension_table_create, company_job_url_dimension_table_create, web_extractor_staging_table_create, web_extractor_error_staging_table_create, web_extractor_hist_staging_table_create, web_extractor_hist_error_staging_table_create, share_price_dimension_table_create, news_dimension_table_create, rank_index_dimension_table_create, service_product_dimension_table_create, revenue_dimension_table_create, hr_dimension_table_create, location_dimension_exclude_keyword_table_create, location_dimension_table_create, job_dimension_table_create, job_data_dimension_table_create, skill_dimension_table_create, company_job_location_fact_table_create, action_log_table_create]
create_sequence_queries = [company_dimension_sequence_create,job_dimension_sequence_create, job_data_dimension_sequence_create, location_dimension_sequence_create]
create_index_queries = [job_dimension_unquie_index_create]








###################################################################################
#NOT USED
xxx_web_extractor_staging_select_count_gpby_url_failure_extract_status = ("""
    select job_url,extract_status,count(*) from web_extractor_staging where job_url ='URL_EXCEPTION_FOUND' or job_url='URL_DATA_NOT_AVAILABLE' group by job_url,extract_status order by count(*) desc;
""")

xxx_web_extractor_error_staging_select_count = ("""
    SELECT count(*) from web_extractor_error_staging;
""")

xxx_web_extractor_staging_select_job_dimension_load = ("""
    SELECT distinct company_name,job_title,job_raw_data from web_extractor_staging where company_name='USAA' and job_raw_data like 'San%..%' and job_title like '%Bank Credit Risk Analyst Senior%' order by company_name asc;
""")

xxx_web_extractor_staging_select_job_dimension_load = ("""
    SELECT distinct company_name,job_title,job_raw_data from web_extractor_staging where company_name='Absa Bank Limited' and job_title='Advisor CFI Cyber Forensic' order by company_name asc;
""")

xxx_web_extractor_staging_select_location_dimension_load = ("""
    SELECT distinct job_raw_data from web_extractor_staging where LENGTH(job_raw_data)-LENGTH(REPLACE(job_raw_data,'|','')) in (0,1,3,4);
""")

xxx_web_extractor_staging_select_job_dimension_load = ("""
    SELECT distinct company_name,job_title,job_raw_data from web_extractor_staging where LENGTH(job_raw_data)-LENGTH(REPLACE(job_raw_data,'|','')) in (0,1,3,4);
""")
 