from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = (SparkSession
         .builder
         .appName("SPARK-EXAMPLE4-2")
         .enableHiveSupport()
         .getOrCreate())

# 创建管理的表
# spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
#
# spark.sql(
#     "CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")
csv_file = "/Users/jackpan/PycharmProjects/sparkpy/data/departuredelays.csv"
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema=schema)

# spark.sql("DROP TABLE managed_us_delay_flights_tbl")
# flights_df.write.option("mode", "override").saveAsTable("managed_us_delay_flights_tbl")
#
# spark.catalog.listDatabases()
# spark.catalog.listTables()
# spark.catalog.listColumns("managed_us_delay_flights_tbl")

# us_flights_df = spark.sql("SELECT * FROM us_delay_fligths_tbl")
# us_flights_df.show(10)
#
# (flights_df.write.option("path", "/Users/jackpan/JackPanDocuments/temporary/data/us_flights_delay")
#  .saveAsTable("us_flights_delay_tbl"))

# 根据数据表进行查询
df_sfo = spark.sql("SELECT distance, origin, destination FROM us_flights_delay_tbl "
                   "WHERE origin == 'SFO'")
df_sfo.show(10)

df_jfk = spark.sql("SELECT distance, origin, destination FROM us_flights_delay_tbl "
                   "WHERE origin == 'JFK'")
df_jfk.show(10)

# 创建临时视图
df_sfo.createOrReplaceTempView("us_flights_delay_tbl_SFO_temp")
spark.sql("SELECT distance, origin, destination FROM us_flights_delay_tbl_SFO_temp "
                   "WHERE origin == 'SFO'").show(10)