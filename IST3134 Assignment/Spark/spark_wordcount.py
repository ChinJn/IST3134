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

review1_rdd = sc.textFile("hdfs:///1_title_reviews")
review1_rdd = review1_rdd.map(lambda x: re.sub(r'\t', '', x))
review1_rdd = review1_rdd.map(lambda x: re.sub(r'[^\w\s]', '', x))

review1_rdd = review1_rdd.flatMap(lambda line: line.split(" "))
review1_rdd = review1_rdd.filter(lambda x: x != '')

# Remove stopwords using NLTK
stop_words = set(stopwords.words('english'))
review1_rdd = review1_rdd.filter(lambda x: x.lower() not in stop_words)

review1_count = review1_rdd.map(lambda word: (word, 1))
review1_wc = review1_count.reduceByKey(lambda x, y: (x + y))

review1_wc.saveAsTextFile('hdfs:///wc1spark')

# Show RunTime
end_time = time.time()
execution_time = end_time - start_time
print("Execution time: {:.2f} seconds".format(execution_time))

sc.stop()

