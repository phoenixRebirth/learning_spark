from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "NetworkWordCount")
sc.setLogLevel("WARN")   # on met le lvel de log à "WARN" AS code
ssc = StreamingContext(sc, 15) # le "15" est l'intervalle de temps au bout duquel on relance le script

lines = ssc.socketTextStream("localhost", 9999)
# salut salut 
# France Paris
# Paris
# ("salut",2),("paris",2), ("France",1)

words = lines.flatMap(lambda x : x.split())
words.count()
#words.pprint()

wordCounts = words.map(lambda x : (x, 1)).reduceByKey(lambda x,y : x+y)

wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate