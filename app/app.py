from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower


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

spark = SparkSession(sc)

# Create DataFrame representing the stream of input lines from connection to host:port
lines = (
    spark.readStream.format("socket")
    .option("host", "data_app")
    .option("port", 9999)
    .load()
)


# Split the lines into words
words = lines.select(
    # explode turns each item in an array into a separate row
    explode(split(lines.value, " ")).alias("word")
)

# lower case the words
words = words.withColumn("word", lower(words.word.cast("string")))

# Generate running word count
wordCounts = words.groupBy("word").count().sort("count", ascending=False)

# Start running the query that prints the running counts to the console
query = wordCounts.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()
