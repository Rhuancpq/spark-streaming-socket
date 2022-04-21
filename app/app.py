from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession


conf = (
    SparkConf()
    .setMaster("spark://spark:7077")
    .setAppName("NetworkWordCount")
    .set("spark.dynamicAllocation.enabled", "false")
    .set("spark.shuffle.service.enabled", "false")
    .set("spark.executor.memory", "512m")
    .set("spark.executor.instances", "2")
)

sc = SparkContext(conf=conf)

ssc = StreamingContext(sc, 1)

spark = SparkSession(sc)

# Create a empty DataFrame
df = spark.createDataFrame([], "words STRING, count INT")

lines = ssc.socketTextStream("data_app", 9999)

words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word.lower(), 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# TODO merge the new data with the old one, sum up counts of the same words and save to dataframe
wordCounts.foreachRDD()

# print the top 10 words
df.orderBy("count", ascending=False).limit(10).show()

ssc.start()  # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
