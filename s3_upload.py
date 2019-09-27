import pandas as pd
import os
import boto3
import configparser
import logging
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession



config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

def upload_csv(bucket_name):
    """Uploads csv files to s3 bucket"""
    
    # Global land temperature csv
    data = open('/data2/GlobalLandTemperaturesByCity.csv', 'rb')
    s3.Bucket(bucket_name).put_object(Key='land_temp/GlobalLandTemperaturesByCity.csv', Body=data)
    data = open('/home/workspace/airport-codes_csv.csv', 'rb')
    # Airport csv
    s3.Bucket(bucket_name).put_object(Key='airport/airport-codes_csv.csv', Body=data)
    data = open('/home/workspace/us-cities-demographics.csv', 'rb')
    # City demo csv
    s3.Bucket(bucket_name).put_object(Key='city_demo/us-cities-demographics.csv', Body=data)
    
    
def upload_sas(bucket_name, spark):
    """Transforms sas files to csv format, uploads csv files to s3 bucket"""
    
    # Read immigration sas file names
    file_list = []
    for dirpath, subdirs, files in os.walk('../../data/18-83510-I94-Data-2016/'):
        for x in files:
            if x.endswith(".sas7bdat"):
                file_list.append(os.path.join(dirpath, x))
                print(os.path.splitext(x)[0])
    
    # Transform sas files to csv format
    for file in file_list:
        df = spark.read.format('com.github.saurfang.sas.spark').load(file)
        df.show(1)
        df.write.csv('sas_test', mode="overwrite", header="true")
    
    # Upload csv files to s3 bucket
    for filename in os.listdir('/home/workspace/sas_test/'):
        if os.path.splitext(filename)[1] == '.csv': # if csv file
            print(os.path.splitext(filename))
            print('True')
            s3.Object(bucket_name, 'imm/'+filename).put(Body=open('/home/workspace/sas_test/'+filename, 'rb'))

    
if __name__ == '__main__':
    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()
    
    upload_csv('airflow-adrian')
    upload_sas(bucket_name='airflow-adrian', spark=spark)
    