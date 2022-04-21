from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext


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

lines = ssc.socketTextStream("127.0.0.1", 9999)

words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()  # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
