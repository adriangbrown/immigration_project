# Project Scope
Purpose:  The goal of this project to import disparate data sources (immigration statistics, airport details, world temperature data, and city demographic information) and output analytics fact and dimension tables that allow the user to understand the qualitative nature of immigration patterns

# Approach:  
-Use AWS s3 to store data, the cloud storage solution will allow for almost limitless storage options in the future along with the ability to easily process data on EMR clusters and automation of everything with Airflow
-Initially manually upload data to s3 with the assumption that subsequent data sets would be uploaded, updated, and maintained within s3.  
-Utilize Spark to transform the raw data into Parquet files.  Spark is the current best-in-class solution for processing large data sets on a distributed cluster.  
-Use AWS EMR clusters to allow for faster processing using distributed computing capabilities and future expansion of data sets.  
-Automate the transformation process in Airflow.  Airflow allows for additional customization compared to a typical cron job and includes other features such as a web interface to monitor pipelines
-AWS Cloud Formation service is an easy way to create the above setup instances and clusters, along with the integration

# Output: Four parquet tables will result from this project:
* immigration fact table that includes information about where and when
* city demographics dimension table joins to immigration table via port_id - look at immigrant city population characteristics to understand makeup of popular immigrant cities
* airport information dimension table joins to immigration table via state:port_state, understand types of airports by count for various immigration states
* city land temperature dimension table joins to immigration table via port_id, learn about immigration city temperature trends

# Addressing Other Scenarios
-Data increases by 100x:  Depending on the size and complexity you might need to utilize additional s3 buckets, expand the EMR clusters to accomodate the increase, and potentially create multiple DAGS to handle the additional data set types.
-Pipelines run daily at 7am:  Update the airflow dag schedule interval in cron fashion (0 7 star star star) to reflect an everyday run occuring at 0700
-Database needs to be accessed by 100+ people:  Update transformation process to create of parquet tables in AWS Redshift.  Manage database security via Redshift Users.

# Steps Taken:

Setup AWS Cloud Formation Stack that creates instances and allows for the ability to store data in s3, process data using Spark and EMR, and automate the pipeline in Airflow
-Things of note:  Ensure your s3 bucket is globally unique, meaning no other bucket is named similarly, save time by attempting to create an s3 bucket with your intended name.  Deletion of stack can sometimes fail, in one case EMR Master and Slave rules may fail if being used by other resources.  Manually delete the underlying rules for each security group and then delete the groups to ensure stack deletion.  Identify resources you wish to retain upon stack deletion such as s3 buckets
-Access airflow by sshing into the ec2 instance and running airflow initdb, scheduler, and finally webserver as a sudo super user in the airflow folder

Transform and upload local files to s3 bucket created in the Cloud Formation Stack into csv format
-Import boto3 to access the s3 bucket on AWS and use the method put_object for csv-ready files.  Use Spark, specifically the Saurfang setup to ingest SAS files, convert to csv and put_object method to upload to s3.

-Enable Airflow DAG that creates an EMR cluster, waits for cluster creation, transforms csv files into s3 parquet tables such that the immigration file can be joined to the dimensional tables, and finally terminates the EMR cluster

# Files to use
* s3_upload.py - Transforms files to csv format and uploads to s3
* sas_labels.py - Transforms SAS labels file into csv in s3
* pipeline_udag.py - Dag details and tasks that run separate python files.  Two main tasks are around EMR creation/deletion and data processing
* airport.py - processes airport data
* city_demo.py - processes city demographic data
* imm.py - processes immigration data
* land_temp.py - processes world land temperatures
* emr_lib.py - creates emr cluster and spark session within airflow

# Data Dictionary for Parquet Tables
Airport
* ident: airport identifier
* type: airport type
* iso_region: airport region
* state: airport state

City Demo
* port_id: city port id
* city_lower: city name
* median_age: median age of population
* male_pop: male population size
* female_pop: female population size
* total_pop: total population size
* veterans_num: veteran population size
* Foreign-Born: foreign-born poulation size
* avg_household: average number of people in each household
* state_code: state abbreviation
* ai_an:  american indian and alaska native population size
* Asian: asian population size
* black_aa: black and african american population size
* h_l: hispanic and latino poulation size
* White: white population size

Immigration
* cicid: unique immigration identifier
* arrival_date: immigrant arrival date
* i94_port: immigrant arrival port city code
* port_city: immigrant arrival city
* month: arrival month
* year: arrival year

Land Temperature
* dt: temperature measurement date
* AverageTemperature: average temperature of city
* city_lower: city name
* port_id: city port id



