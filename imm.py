from pyspark.sql.functions import udf, col, lower, lit, to_date, current_date, datediff, split, trim, expr, year, month

# Read ports csv
ports_df = spark.read.csv('s3a://airflow-adrian/ports/part-00000-9584fffb-0ff2-48a5-a13a-b0bbc095f7fa-c000.csv', header=True)

# Read s3 bucket folder with immigration csv files
files = 's3a://airflow-adrian/imm/*.csv'
imm_df = spark.read.csv(files, header=True)
# Hardcode date field to help with sas date transformation
imm_df = imm_df.withColumn('sas_date', to_date(lit('1960-01-01')))
# Convert sas date into proper date format
imm_df = imm_df.withColumn('arrival_date', expr("date_add(sas_date, arrdate)"))
# Create year column
imm_df = imm_df.withColumn('year', year(col('arrival_date')))
# Create month column
imm_df = imm_df.withColumn('month', month(col('arrival_date')))
# Drop duplicate rows - Data Quality Check
imm_df = imm_df.dropDuplicates()
# Join ports data
imm_df = imm_df.join(ports_df, (imm_df.i94port==ports_df.port_id))
# Write parquet tables to s3
imm_df.select('cicid', 'arrival_date', 'i94port', col('city').alias('port_city'), col('state').alias('port_state'), 'year', 'month').write.partitionBy('year').parquet('s3a://airflow-adrian/parquet_tables/imm/', mode='overwrite')

# Check size of table
imm = 's3a://airflow-adrian/parquet_tables/imm/*/*.snappy.parquet'
imm_df = spark.read.option('basePath', 's3a://airflow-adrian/parquet_tables/imm/*/*.snappy.parquet').parquet(imm)
print imm_df.count()