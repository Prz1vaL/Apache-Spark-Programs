from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parseline(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return age, num_friends


lines = sc.textFile("/Users/apm30/PycharmProjects/Average-Spark-Program/fakefriends.csv")
rdd = lines.map(parseline)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
ageByGroup = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
ageResults = ageByGroup.collect()
for result in results:
    print(result)

for result in ageResults:
    print("The total no. of people in each age category are", ageResults)
    break
