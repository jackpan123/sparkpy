from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .getOrCreate())


def to_date_format_udf(d_str: str) -> str:
    l = [char for char in d_str]
    return "".join(l[0:2]) + "/" + "".join(l[2:4]) + " " + " " + "".join(l[4:6]) + ":" + "".join(l[6:])


to_date_format_udf("02190925")
spark.udf.register("to_date_format_udf", to_date_format_udf, StringType())

csv_file = "/Users/jackpan/PycharmProjects/sparkpy/data/departuredelays.csv"

df = (spark.read.format("csv")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .load(csv_file))
df.selectExpr("to_date_format_udf(date) as date_format").show(10, truncate=False)
df.createOrReplaceTempView("us_delay_flights_tbl")

# spark.sql("CACHE TABLE us_delay_flights_tbl")
# spark.sql("SELECT *, date, to_date_format_udf(date) AS date_fm FROM us_delay_flights_tbl").show(10, truncate=False)

# 查找所有所有航班超出1000英里的航班
spark.sql("SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC").show(10)
# 等价的DataFrame API
df.select("distance", "origin", "destination").filter(col("distance") > 1000).orderBy(desc("distance")).show(10)

# 查找旧金山（SFO机场）和芝加哥（ORD机场）间所有延误超过两小时的航班
# spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl "
#           "WHERE delay > 120 AND origin == 'SFO' AND destination == 'ORD' ORDER BY delay DESC").show(10)
#
# # 转换不同的表达方式
# spark.sql("""SELECT delay, origin, destination,
#               CASE
#                   WHEN delay > 360 THEN 'Very Long Delays'
#                   WHEN delay > 120 AND delay < 360 THEN  'Long Delays '
#                   WHEN delay > 60 AND delay < 120 THEN  'Short Delays'
#                   WHEN delay > 0 and delay < 60  THEN   'Tolerable Delays'
#                   WHEN delay = 0 THEN 'No Delays'
#                   ELSE 'No Delays'
#                END AS Flight_Delays
#                FROM us_delay_flights_tbl
#                ORDER BY origin, delay DESC""").show(10, truncate=False)
