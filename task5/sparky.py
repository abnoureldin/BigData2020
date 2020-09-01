from pyspark import SparkContext

sc = SparkContext(appName="Shakespeare")

rdd = sc.textFile("/home/hadoop/BigData2020/Shakespeare.txt")
rdd.take(5)

def Reader(lines):
	lines = lines.lower()
	lines = lines.split()
	return lines

rdd1 = rdd.map(Reader)
rdd1.take(5)
