from pyspark import SparkConf, SparkContext

# Declaring the spark variables
conf = SparkConf().setMaster("local").setAppName("Customer-Spark")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    dollarAmount = float(fields[2])
    return customerID, dollarAmount


lines = sc.textFile("customer-orders.csv")
parsedLine = lines.map(parseLine)
# customerInfo = parsedLine.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
cinfo = lines.map(parseLine)
# dollarPerCustomer = customerInfo.mapValues(lambda x: x[0] + x[1])
dpc = cinfo.reduceByKey(lambda x, y: x + y)
# results = dollarPerCustomer.collect()

resultsFlipped = dpc.map(lambda x: (x[1], x[0]))
total = resultsFlipped.sortByKey()

results = total.collect()
for result in results:
    print(result)
