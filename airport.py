from pyspark.sql.functions import udf, col, lower, lit, to_date, current_date, datediff, split, trim

# Read airport csv
airport_df = spark.read.csv('s3a://airflow-adrian/airport-codes_csv.csv', header=True)
# Extract state code from iso region field to allow for joins
airport_df = airport_df.withColumn('iso_region_split', split(airport_df.iso_region, '-')[1])
# Drop duplicate rows - Data Quality Check
airport_df = airport_df.dropDuplicates()
# Write parquet tables to s3
airport_df.select('ident', 'type', 'iso_region', col('iso_region_split').alias('state')).write.parquet('s3a://airflow-adrian/parquet_tables/airport/', mode='overwrite')

# Check size of table
airport = 's3a://airflow-adrian/parquet_tables/airport/*.snappy.parquet'
airport_df = spark.read.option('basePath', 's3a://airflow-adrian/parquet_tables/airport/*.snappy.parquet').parquet(airport)
print airport_df.count()



