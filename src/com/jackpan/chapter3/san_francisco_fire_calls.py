from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from IPython.display import display

spark = (SparkSession
         .builder
         .appName("SanFranciscoFileCalls")
         .getOrCreate())

sf_fire_file = "/Users/jackpan/PycharmProjects/sparkpy/data/sf-fire-calls.csv"

fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField('City', StringType(), True),
                          StructField('Zipcode', IntegerType(), True),
                          StructField('Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField('FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])

fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

fire_df.show(10)
# fire_df.write.format("parquet").save("/Users/jackpan/JackPanDocuments/temporary/springlogtest/test1")

fire_df.cache()

fire_df.count()

fire_df.printSchema()

# display(fire_df.limit(5))

few_fire_df = (fire_df.select("IncidentNumber", "AvailableDtTm", "CallType")
               .where(col("CallType") != "Medical Incident"))

few_fire_df.show(5, truncate=False)

# 返回不同的报警类型的数量
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .agg(countDistinct("CallType").alias("DistinctCallTypes"))
 .show())

# 列出数据集中不同的出警类型
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .distinct()
 .show(10, False))