import pyspark
import sys

print("Hello, world!")

master_url = sys.argv[1] if len(sys.argv) >= 2 else "local"
sc = pyspark.SparkContext(master_url)

msg = "Hello, pyspark!"
rdd = sc.parallelize(list(msg))
print(''.join(rdd.collect()))
