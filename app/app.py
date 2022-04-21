from cProfile import run
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext


conf = (
    SparkConf()
    .setMaster("spark://spark:7077")
    .setAppName("NetworkWordCount")
    .set("spark.dynamicAllocation.enabled", "false")
    .set("spark.shuffle.service.enabled", "false")
    .set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
    .set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true")
    .set("spark.executor.memory", "512m")
    .set("spark.executor.instances", "2")
)

sc = SparkContext(conf=conf)

ssc = StreamingContext(sc, 1)

ssc.checkpoint("checkpoint")

state_rdd = sc.emptyRDD()


def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


lines = ssc.socketTextStream("data_app", 9999)

words = lines.flatMap(lambda line: line.split(" "))

pairs = words.map(lambda word: (word.lower(), 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

running = wordCounts.updateStateByKey(updateFunc, initialRDD=state_rdd)

running.pprint()

ssc.start()  # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
