from pyspark import SparkContext

sc = SparkContext(appName="Shakespeare")

rdd = sc.textFile("/home/hadoop/BigData2020/Shakespeare.txt")
print(rdd.take(20))

def Reader(lines):
	lines = lines.lower()
	lines = lines.split()
	return lines

rdd1 = rdd.map(Reader)
print(rdd1.take(20))

rdd2 = rdd1.map(lambda x: len(x))
print("line count:",str(rdd2.take(20)))
