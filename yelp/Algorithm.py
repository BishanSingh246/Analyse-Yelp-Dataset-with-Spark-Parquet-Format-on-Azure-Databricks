# Databricks notebook source
# MAGIC %run /yelp/Auth

# COMMAND ----------

#import packages
import pyspark.sql.functions as f
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Read yelp Datasets from ADLS(Azure Data Lake Storage) and convert it from JSON to Delta format.
df_business_delta = spark.read.json("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/yelp_academic_dataset_business.json")
df_business_delta.write.mode("overwrite").parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/json_to_delta/business.delta")

# COMMAND ----------

# DBTITLE 1,Read yelp Datasets from ADLS(Azure Data Lake Storage) and convert it from JSON to Parquet.
df_business = spark.read.json("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/yelp_academic_dataset_business.json")
df_business.write.mode("overwrite").parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/json_to_parquet/business.parquet")

df_checkin = spark.read.json("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/yelp_academic_dataset_checkin.json")
df_checkin.write.mode("overwrite").parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/json_to_parquet/checkin.parquet")

df_review = spark.read.json("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/yelp_academic_dataset_review.json")
df_review.write.mode("overwrite").parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/json_to_parquet/review.parquet")

df_tip = spark.read.json("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/yelp_academic_dataset_tip.json")
df_tip.write.mode("overwrite").parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/json_to_parquet/tip.parquet")

df_user = spark.read.json("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/yelp_academic_dataset_user.json")
df_user.write.mode("overwrite").parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/json_to_parquet/user.parquet")

# COMMAND ----------

display(df_business_delta)

# COMMAND ----------

display(df_business)

# COMMAND ----------

# DBTITLE 1,Reading Parquet File
df_business = spark.read.parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/json_to_parquet/business.parquet")
df_checkin = spark.read.parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/json_to_parquet/checkin.parquet")
df_review = spark.read.parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/json_to_parquet/review.parquet")
df_tip = spark.read.parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/json_to_parquet/tip.parquet")
df_user = spark.read.parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/json_to_parquet/user.parquet")

# COMMAND ----------

# DBTITLE 1,Total Records in Each Datasets
print("df_business: "+str(df_business.count()))
print("df_checkin: "+str(df_checkin.count()))
print("df_review: "+str(df_review.count()))
print("df_tip: "+str(df_tip.count()))
print("df_user: "+str(df_user.count()))

# COMMAND ----------

display(df_tip)

# COMMAND ----------

#import packages
import pyspark.sql.functions as f
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Extract year and month  from Date Column
# Extacting Year from date column and creating new Column name tip_year
df_tip = df_tip.withColumn("tip_year",f.year(f.to_date(f.col("date"))))
# Extacting month from date column and creating new Column name tip_month
df_tip = df_tip.withColumn("tip_month",f.month(f.to_date(f.col("date"))))
display(df_tip)

# COMMAND ----------

# DBTITLE 1,Partition Dataset tip by date column
df_tip.write.mode("overwrite").partitionBy("tip_year", "tip_month").parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/tip_partitioned_by_year_and_month/")

# COMMAND ----------

# DBTITLE 1,Coalesce() and repartition()
# getNumPartition will fetching no of partitions in dataframe and before that we need to converting dataframe into rdd.
print("current number of partitions: " + str(df_user.rdd.getNumPartitions()))
# coalesce uses existing partition to minimize the amount of data that's shuffled.
# repartition creates new partition and dose full shuffel of data and it creates roughly equal size partitions.
# coalesce is only used to reduce partitions but by using repartitions we can increase the number of partitions.
# coalesce is fast but partition size is not equal and repartition is slow then coalesce but it creates mostly equal size partitions.
df_reduce_part = df_user.coalesce(10)
print("reduce number of partition after coalesce: " + str(df_reduce_part.rdd.getNumPartitions()))

df_increase_part = df_user.repartition(30)
print("increase number of partition: " + str(df_increase_part.rdd.getNumPartitions()))

# COMMAND ----------

# DBTITLE 1,Coalesce
# coalesce is fast but each partition size is diffrents
df_user.coalesce(10).write.mode("overwrite").parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/coalese/user.parquet")

# COMMAND ----------

# DBTITLE 1,Repartition
# repartition is fast but each partition size is similar
df_user.repartition(10).write.mode("overwrite").parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/repartition/user.parquet")
display(df_user)

# COMMAND ----------

df_user = spark.read.parquet("abfss://yelpdataset002@sayelp001.dfs.core.windows.net/json_to_parquet/user.parquet")
display(df_user)

# COMMAND ----------

# DBTITLE 1,creating temporary view of a DataFrame in memory
df_user.createOrReplaceTempView("user")

# COMMAND ----------

# DBTITLE 1,Find top 3 users based on their total number of reviews:
# MAGIC %sql
# MAGIC select user_id,name,review_count from user order by review_count desc limit 3;
# MAGIC -- highest review_count users will be listed first as it is a descending

# COMMAND ----------

# DBTITLE 1,Finding the top 10 users with the most fans:
# MAGIC %sql
# MAGIC select user_id, name, fans from user order by fans desc limit 10 
# MAGIC -- arrange user_id in descending order of number of fans
# MAGIC -- limit to first 10 rows

# COMMAND ----------

# DBTITLE 1,Business DataFrame
display(df_business)

# COMMAND ----------

# DBTITLE 1,Analyse top 10 catagories by number of reviews
# using groupby
df_business_cat= df_business.groupBy("categories").agg(f.count("review_count").alias("total_review_count"))
# adding rnk column
df_top_categories = df_business_cat.withColumn("rnk", f.row_number().over(Window.orderBy(f.col('total_review_count').desc())))
df_top_categories = df_top_categories.filter(f.col('rnk') <= 10)
display(df_top_categories)

# COMMAND ----------

# DBTITLE 1,creating  temporary view of a DataFrame in memory of business dataframe
df_business.createOrReplaceTempView("business")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT categories, total_review_count FROM (
# MAGIC     SELECT business_categories.*, 
# MAGIC     ROW_NUMBER() OVER (ORDER BY total_review_count DESC) rn 
# MAGIC     FROM (SELECT categories, count(review_count) AS total_review_count FROM business GROUP BY categories) business_categories
# MAGIC )
# MAGIC WHERE rn <= 10

# COMMAND ----------

# DBTITLE 1,Analyse top business which have over 1000 reviews
df_business_reviews = df_business.groupBy("categories").agg(f.count("review_count").alias("total_review_count"))
df_top_business = df_business_reviews.filter(df_business_reviews["total_review_count"] >= 1000).orderBy(f.desc("total_review_count"))

# COMMAND ----------

display(df_top_business)

# COMMAND ----------

# DBTITLE 1,Analyse Business data : Number of resturant per state
df_num_of_restaurants = df_business.select('state').groupBy('state').count().orderBy(f.desc("count"))
display(df_num_of_restaurants)

# COMMAND ----------

# DBTITLE 1,Analyze top 3 Resturant in each state
df_business.createOrReplaceTempView("business_restaurant")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC     SELECT STATE,name,review_count,
# MAGIC     ROW_NUMBER() OVER ( PARTITION BY STATE ORDER BY review_count DESC) rn 
# MAGIC     FROM business_restaurant
# MAGIC )
# MAGIC WHERE rn <= 3

# COMMAND ----------

# DBTITLE 1,List the top resturant in a state by the number of reviews
df_business_Arizona = df_business.filter(df_business['state']=='AZ')
df_Arizona = df_business_Arizona.groupBy("name").agg(f.count("review_count").alias("total_review_count"))
window = Window.orderBy(df_Arizona['total_review_count'].desc())
df_Arizona_best_rest = df_Arizona.select('*', f.rank().over(window).alias('rank')).filter(f.col('rank') <= 10)
display(df_Arizona_best_rest)

# COMMAND ----------

# DBTITLE 1,Number of resturant in Arizona State per city
df_business_Arizona = df_business.filter(df_business['state']=='AZ')
df_business_Arizona = df_business_Arizona.groupBy('city').count().orderBy(f.desc("count"))
#Visualise number of restaurants in Arizona City, city Phoneix can be visualised from here that is having maximum number of restaurants
display(df_business_Arizona)

# COMMAND ----------

# DBTITLE 1,Select City with highest number of restaurants
window = Window.orderBy(df_business_Arizona['count'].desc())
city_with_max_rest = df_business_Arizona \
                    .select('*', f.rank().over(window).alias('rank')).filter(f.col('rank') <= 1) \
                    .drop('rank')
display(city_with_max_rest)

# COMMAND ----------

# DBTITLE 1,Broadcast Join
from pyspark.sql.functions import broadcast

# broadcast join is use when one table is big and another table is small in size.

#Join arizona city dataset with business datasets to get more details
df_best_restaurants = df_business.join(broadcast(city_with_max_rest),"city", 'inner')

df_best_restaurants = df_best_restaurants.groupBy("name","stars").agg(f.count("review_count").alias("review_count"))

df_best_restaurants = df_best_restaurants.filter(df_best_restaurants["review_count"] >= 10)
df_best_restaurants = df_best_restaurants.filter(df_best_restaurants["stars"] >= 3)

#Visualize restaurants as per review ratings in Pheonix city
display(df_best_restaurants)

# COMMAND ----------

# DBTITLE 1,Most rated Italian resturant in pheonix
df_business_Pheonix = df_business.filter(df_business.city == 'Phoenix')
df_business_italian = df_business_Pheonix.filter(df_business.categories.contains('Italian'))
display(df_business)

df_best_italian_restaurants = df_business_italian.groupBy("name").agg(f.count("review_count").alias("review_count"))

# df_best_italian_restaurants = df_best_italian_restaurants.filter(df_best_italian_restaurants["review_count"] >= 5)
df_best_italian_restaurants = df_best_italian_restaurants.filter(df_best_italian_restaurants["review_count"] >= 1)
display(df_best_italian_restaurants)

# COMMAND ----------

# DBTITLE 1,Most rated Italian resturant in Tucson
df_business_Tucson = df_business.filter(df_business.city == 'Tucson')
df_business_italian = df_business_Tucson.filter(df_business.categories.contains('Italian'))
display(df_business_Tucson)
display(df_business_italian)
df_best_italian_restaurants = df_business_italian.groupBy("name").agg(f.count("review_count").alias("review_count"))
display(df_best_italian_restaurants)
df_best_italian_restaurants = df_best_italian_restaurants.filter(df_best_italian_restaurants["review_count"] >= 5)
display(df_best_italian_restaurants)