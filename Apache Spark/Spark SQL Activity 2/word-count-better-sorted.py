# import spark sql functions

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

# Create the Spark App
spark = SparkSession.builder.appName("Word-Count").getOrCreate()

# Read each line of the given txt file into a dataframe.

inputDataFrame = spark.read.text("book.txt")

# Since the data is unstructured , split using a regular expression that extracts the words.

words = inputDataFrame.select(func.explode(func.split(inputDataFrame.value, "\\W+")).alias("word"))
words.filter(words.word != "")

# Normalise everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# Count the number of occurrences of a word in the book
wordCount = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountSortedbyWord = wordCount.sort("word")
wordCountSortedbyCount = wordCount.sort("count")

# Show the results by Word
wordCountSortedbyWord.show(wordCountSortedbyWord.count())

# Show the results by Count
wordCountSortedbyCount.show(wordCountSortedbyCount.count())
