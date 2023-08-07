from pyspark.sql import SparkSession
import nltk
import re
import time
from nltk.corpus import stopwords

# Download the stopwords list from NLTK
nltk.download('stopwords')

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("WordCount") \
    .getOrCreate()

sc = spark.sparkContext

start_time = time.time()

review5_rdd = sc.textFile("hdfs:///5_title_reviews")
review5_rdd = review5_rdd.map(lambda x: re.sub(r'\t', '', x))
review5_rdd = review5_rdd.map(lambda x: re.sub(r'[^\w\s]', '', x))

review5_rdd = review5_rdd.flatMap(lambda line: line.split(" "))
review5_rdd = review5_rdd.filter(lambda x: x != '')

# Remove stopwords using NLTK
stop_words = set(stopwords.words('english'))
review5_rdd = review5_rdd.filter(lambda x: x.lower() not in stop_words)

review5_count = review5_rdd.map(lambda word: (word, 1))
review5_wc = review5_count.reduceByKey(lambda x, y: (x + y))

review5_wc.saveAsTextFile('hdfs:///wc5spark')

# Show RunTime
end_time = time.time()
execution_time = end_time - start_time
print("Execution time: {:.2f} seconds".format(execution_time))

sc.stop()
