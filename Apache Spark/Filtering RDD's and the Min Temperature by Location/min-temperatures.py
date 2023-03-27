from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return stationID, entryType, temperature


lines = sc.textFile("/Users/apm30/Documents/Git/Filtering RDD's and the Min Temperature by Location/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
maxStationTemps = maxTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))
maxTemps = maxStationTemps.reduceByKey(lambda x, y: max(x, y))
results = minTemps.collect()
maxResults = maxTemps.collect()
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

for i in maxResults:
    print(i[0] + "\t{:.2f}F".format(i[1]))


