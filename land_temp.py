from pyspark.sql.functions import udf, col, lower, lit, to_date, current_date, datediff, split, trim

# Read port csv
ports_df = spark.read.csv('s3a://airflow-adrian/ports/part-00000-9584fffb-0ff2-48a5-a13a-b0bbc095f7fa-c000.csv', header=True)

# Read land temp csv
land_temp_df = spark.read.csv('s3a://airflow-adrian/land_temp/GlobalLandTemperaturesByCity.csv', header=True, inferSchema=True)
# Calculate days between current date and dt column to allow for filtering
land_temp_df = land_temp_df.withColumn('date_diff', datediff(current_date(), col("dt")))
# Filter dataframe for most recent 9000 days of data
land_temp_df = land_temp_df.filter(land_temp_df.date_diff < 9000)
# Lower city column to allow for join
land_temp_df = land_temp_df.withColumn('city_lower', lower(col('City')))
# Drop duplicate rows - Data Quality Check
land_temp_df = land_temp_df.dropDuplicates()
# Join ports data
land_temp_df = land_temp_df.join(ports_df, (land_temp_df.city_lower==ports_df.city))
# Write parquet tables to s3 
land_temp_df.select('dt', 'AverageTemperature', 'city_lower', 'port_id').write.partitionBy('dt').parquet('s3a://airflow-adrian/parquet_tables/land_temp/', mode='overwrite')

# Check size of table
land_temp = 's3a://airflow-adrian/parquet_tables/land_temp/*/*.snappy.parquet'
land_temp_df = spark.read.option('basePath', 's3a://airflow-adrian/parquet_tables/land_temp/*.snappy.parquet').parquet(land_temp)
print land_temp_df.count()