import pandas as pd
import re
import os
import configparser

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lower, lit, to_date, current_date, datediff, split, trim

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
        
spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

with open('/home/workspace/I94_SAS_Labels_Descriptions.SAS', 'r') as f:
    file = f.read()
    # Search file for port details and extract matches
    ports_text = re.search(r'I94PORT.+?;', file, re.DOTALL).group(0)
    matches = re.findall(r'(.+)=(.+)', ports_text)
    matches.sort(key=lambda m: m[0])
    ports = []

    for (portid, detail) in matches:
      ports.append((portid.strip(), detail.strip()[1:-1]))
    # Create dataframe
    ports_df = pd.DataFrame(ports, columns=['port_id', 'city_state'])
    ports_df['port_id'] = ports_df['port_id'].replace('[\',)]','', regex=True)
    # Create dataframe with separate city and state field
    new_df = ports_df['city_state'].str.split(",", n = 1, expand = True)
    new_df.columns = ['city', 'state']
    # Lowercase to allow for joining of tables
    new_df.city = new_df.city.str.lower()
    # Combine dataframes
    source_df = pd.concat([ports_df, new_df], axis=1)
    # Convert pandas dataframe to spark dataframe
    ports_sdf = spark.createDataFrame(source_df)
    ports_sdf = ports_sdf.withColumn('state', trim(col('state')))
    # Write dataframe to csv in s3 bucket
    ports_sdf.write.csv('s3a://airflow-adrian/ports', header='true', mode='overwrite')
