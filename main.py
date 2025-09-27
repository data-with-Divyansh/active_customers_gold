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

cust_df = (spark.read.csv("customers.csv",sep=",",header="true")
           .filter("status = 'active'"))
cust_df.show()

order_df = spark.read.csv("orders.csv",sep=",",header="true")
order_df.show()

joindf = cust_df.join(order_df,"customer_id","inner")
joindf.show()

totaldf = joindf.groupBy("customer_id").agg(sum("amount").alias("total_spend"))
# totaldf.show()

join2 = joindf.join(totaldf,"customer_id","left").withColumn("tier",expr("case when total_spend > 500 then 'Gold' else 'Silver' end"))
join2.show()

# join2.write.format("csv").mode("append").partitionBy("signup_date").save("tier_data")


