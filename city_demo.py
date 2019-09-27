from pyspark.sql.functions import udf, col, lower, lit, to_date, current_date, datediff, split, trim

# Read ports csv
ports_df = spark.read.csv('s3a://airflow-adrian/ports/part-00000-9584fffb-0ff2-48a5-a13a-b0bbc095f7fa-c000.csv', header=True)

# Read city demo csv
city_demo_df = spark.read.csv('s3a://airflow-adrian/us-cities-demographics.csv', header=True, inferSchema=True, sep=';')
# Lower city column to allow for joins
city_demo_df = city_demo_df.withColumn('city_lower', lower(col('City')))
# Transpose race column into individual columns, eliminating duplicated data with groupby clause
city_demo_df = city_demo_df.groupby('City', 'State', 'Median Age','Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-Born', 'Average Household Size', 'State Code', 'city_lower').pivot('Race').max('Count').fillna(0)
# Drop duplicate rows - Data Quality Check
city_demo_df = city_demo_df.dropDuplicates()
# Join ports with city demo to allow for port id inclusion (ties to immigration data)
city_demo_df = city_demo_df.join(ports_df, (city_demo_df.city_lower==ports_df.city))
# Write parquet tables to s3
city_demo_df.select('port_id', 'city_lower', col('Median Age').alias('median_age'), col('Male Population').alias('male_pop'), col('Female Population').alias('female_pop'), col('Total Population').alias('total_pop'), col('Number of Veterans').alias('veterans_num'), 'Foreign-Born', col('Average Household Size').alias('avg_household'), col('State Code').alias('state_code'), col('American Indian and Alaska Native').alias('ai_an'), 'Asian', col('Black or African-American').alias('black_aa'), col('Hispanic or Latino').alias('h_l'), 'White').write.parquet('s3a://airflow-adrian/parquet_tables/city_demo/', mode='overwrite')

# Check size of table
city_demo = 's3a://airflow-adrian/parquet_tables/city_demo/*.snappy.parquet'
city_demo_df = spark.read.option('basePath', 's3a://airflow-adrian/parquet_tables/city_demo/*.snappy.parquet').parquet(city_demo)
print city_demo_df.count()