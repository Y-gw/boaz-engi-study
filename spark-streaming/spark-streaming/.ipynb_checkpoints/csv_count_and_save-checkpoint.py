from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import DataFrame

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("csvCount") \
    .config("spark.driver.extraClassPath", "/usr/local/spark-3.3.1-bin-hadoop3/jars") \
    .getOrCreate()

# df를 jdbc에 저장하는 함수
def writeToJDBC(df: DataFrame, epochId: int):
    # JDBC 연결 설정
    database_url = "jdbc:postgresql://postgres:5432/boaz"
    database_properties = {
        "user": "boaz",
        "password": "boaz",
        "driver": "org.postgresql.Driver"
    }
    
    # 데이터를 데이터베이스에 저장
    df.write.jdbc(url=database_url, table="IRISCount", mode="overwrite", properties=database_properties)

# 스키마 정의
schema = StructType([
    StructField("year", DoubleType(), True),
    StructField("caseno", DoubleType(), True),
    StructField("SepalLength", DoubleType(), True),
    StructField("SepalWidth", DoubleType(), True),
    StructField("PetalLength", DoubleType(), True),
    StructField("PetalWidth", DoubleType(), True),
    StructField("Species", StringType(), True)
])

# 스키마 정의
streaming = spark.readStream\
                .schema(schema)\
                .option("header", True) \
                .option("maxFilesPerTrigger", 1)\
                .csv("./mldata")

# 종(species) 별로 count 후 postgres에 저장
ageCounts = streaming.groupBy(["year","Species"]).count()
ageQuery = ageCounts\
                .writeStream \
                .outputMode("complete") \
                .foreachBatch(writeToJDBC).start()

ageQuery.awaitTermination()

spark.stop()
