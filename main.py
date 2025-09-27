import os
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = 'hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Quantumn\.jdks\corretto-1.8.0_462'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

print("====================== pyspark started ==================")
print()

# Read customers.csv file and filter active customers only as dataframe
cust_df = (spark.read.csv("customers.csv",sep=",",header="true")
           .filter("status = 'active'"))
cust_df.show()

# Read the orders.csv file as dataframe
order_df = spark.read.csv("orders.csv",sep=",",header="true")
order_df.show()

# Join both the dataframes
joindf = cust_df.join(order_df,"customer_id","inner")
joindf.show()

# Find the total amount each customer has spent
totaldf = joindf.groupBy("customer_id").agg(sum("amount").alias("total_spend"))
totaldf.show()

# Add column to mark customers who spent more than 500 as Gold and remaining as Silver
join2 = joindf.join(totaldf,"customer_id","left").withColumn("tier",expr("case when total_spend > 500 then 'Gold' else 'Silver' end"))
join2.show()

# join2.write.format("csv").mode("append").partitionBy("signup_date").save("tier_data")


