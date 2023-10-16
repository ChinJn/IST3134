# Big-Data-Assignment

Dataset From Kaggel
Link: https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews

# Upload and Import Data

1. Import Data to Ubuntu by using WindowSCP

2. Login as Ubuntu

3. From Ubuntu, Copy Data to /tmp
   
```
cp -r books_rating.zip /tmp
```
4. Change User to Hadoop

```
sudo su - hadoop
```
```
start-all.sh
```
5. cd into /tmp
```
cd /tmp
```
6. Copy data from /tmp to home/user/Hadoop
```
cp -r books_rating.zip /home/user/hadoop
```
7. Create a new file "workspace" at hoome directory
```
cd ..
cd home
mkdir wordspace
```
8. Copy zip file to workspace file
```
cp -r books_rating.zip /home/hadoop/workspace
```
9. CD into workspace
```
cd workspace
```
10. Unzip books_rating.zip file
```
unzip books_rating.zip
```
11. Start hive toolscript
```
schematool -initSchema -dbType derby
```
14. Start hive shell
```
hive
```

# Insert data into hive

1. Create Table
```
CREATE TABLE IF NOT EXISTS booksrating2 (id int, title string, price float, user_id string, ProfileName string, helpfulness string, score float, time2 int, summary string, text string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';
```

2. Load Data into table created
```
LOAD DATA LOCAL INPATH '/home/hadoop/workspace/Books_rating.csv' INTO TABLE booksrating2
```

# Data Filtering

3. Create Table (for fitered data)
```
CREATE TABLE IF NOT EXISTS booksrating (id int, title string, price float, user_id string, ProfileName string, helpfulness string, score float, time2 int, summary string, text string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';
```
4. Load Data into table
```
INSERT INTO TABLE booksrating SELECT * FROM booksrating2 WHERE score <= 5.0;
```

5. Create table (score = 4)
```
CREATE TABLE IF NOT EXISTS 4_rating_reviews (title string, user_id string, score float, summary string, text string);
```

6. Insert Data to table (score =4)
```
INSERT INTO 4_rating_reviews
SELECT title, user_id, score, summary, text 
FROM booksrating
WHERE score = 4.0;
```

7. Check if table 4_rating_reviews insert the right data.
```
SELECT DISTINCT SCORE FROM 4_rating_reviews;
```

8. Create table (score = 5)
```
CREATE TABLE IF NOT EXISTS 5_rating_reviews (title string, user_id string, score float, summary string, text string);
```

9. Insert Data to table (score =5)
```
INSERT INTO 5_rating_reviews
SELECT title, user_id, score, summary, text 
FROM booksrating
WHERE score = 5.0;
```

10. Check if table 5_rating_reviews insert the right data.
```
SELECT DISTINCT SCORE FROM 5_rating_reviews;
```

# Create table with only variable (title and text)

11. Create table (title and reviews 4.0 only)
```
CREATE TABLE IF NOT EXISTS 4_title_reviews (title string, text string);
```

12. Insert Data to table (title and reviews 4.0 only)
```
INSERT INTO 4_title_reviews
SELECT title, concat_ws(' ', collect_list(text)) AS concatenated_text
FROM 4_rating_reviews
GROUP BY title;
```

13. Create table (title and reviews 5.0 only)
```
CREATE TABLE IF NOT EXISTS 5_title_reviews (title string, text string);
```

12. Insert Data to table (title and reviews 5.0 only)
```
INSERT INTO 5_title_reviews
SELECT title, concat_ws(' ', collect_list(text)) AS concatenated_text
FROM 5_rating_reviews
GROUP BY title;
```

Insert RATING 1
```
hive> INSERT INTO 1_title_reviews
    > SELECT title, concat_ws(' ', collect_list(text)) AS concatenated_text
    > FROM booksrating
    > WHERE SCORE = 1.0
    > GROUP BY title;
```

# Export Filtered Data 

1. Export to current directory

4_title_reviews
```
INSERT OVERWRITE LOCAL DIRECTORY './4_title_reviews'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT *
FROM 4_title_reviews;
```
5_title_reviews
```
INSERT OVERWRITE LOCAL DIRECTORY './5_title_reviews'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT *
FROM 5_title_reviews;
```

# Upload Filtered Data to HDFS server

4_title_reviews
```
hadoop fs -put 4_title_reviews /
```

5_title_reviews
```
hadoop fs -put 5_title_reviews /
```


# Start Creating mrjob_wordcount.py 

1. Create Py file
```
Nano mrjob_wordcount.py
```

2. Code in mrjob_wordcount.py
```
import re
from mrjob.job import MRJob

class MRWordCount(MRJob):
    def clean_text(self, text):
        # Define a list of stopwords
        stopwords = [‘i’, ‘me’, ‘my’, ‘myself’, ‘we’, ‘our’, ‘ours’, ‘ourselves’, ‘you’, “you’re”, “you’ve”, “you’ll”, “you’d”, ‘your’, ‘yours’, ‘yourself’, ‘yourselves’, ‘he’, ‘him’, ‘his’, ‘himself’, ‘she’, “she’s”, ‘her’, ‘hers’, ‘herself’, ‘it’, “it’s”, ‘its’, ‘itself’, ‘they’, ‘them’, ‘their’, ‘theirs’, ‘themselves’, ‘what’, ‘which’, ‘who’, ‘whom’, ‘this’, ‘that’, “that’ll”, ‘these’, ‘those’, ‘am’, ‘is’, ‘are’, ‘was’, ‘were’, ‘be’, ‘been’, ‘being’, ‘have’, ‘has’, ‘had’, ‘having’, ‘do’, ‘does’, ‘did’, ‘doing’, ‘a’, ‘an’, ‘the’, ‘and’, ‘but’, ‘if’, ‘or’, ‘because’, ‘as’, ‘until’, ‘while’, ‘of’, ‘at’, ‘by’, ‘for’, ‘with’, ‘about’, ‘against’, ‘between’, ‘into’, ‘through’, ‘during’, ‘before’, ‘after’, ‘above’, ‘below’, ‘to’, ‘from’, ‘up’, ‘down’, ‘in’, ‘out’, ‘on’, ‘off’, ‘over’, ‘under’, ‘again’, ‘further’, ‘then’, ‘once’, ‘here’, ‘there’, ‘when’, ‘where’, ‘why’, ‘how’, ‘all’, ‘any’, ‘both’, ‘each’, ‘few’, ‘more’, ‘most’, ‘other’, ‘some’, ‘such’, ‘no’, ‘nor’, ‘not’, ‘only’, ‘own’, ‘same’, ‘so’, ‘than’, ‘too’, ‘very’, ‘s’, ‘t’, ‘can’, ‘will’, ‘just’, ‘don’, “don’t”, ‘should’, “should’ve”, ‘now’, ‘d’, ‘ll’, ‘m’, ‘o’, ‘re’, ‘ve’, ‘y’, ‘ain’, ‘aren’, “aren’t”, ‘couldn’, “couldn’t”, ‘didn’, “didn’t”, ‘doesn’, “doesn’t”, ‘hadn’, “hadn’t”, ‘hasn’, “hasn’t”, ‘haven’, “haven’t”, ‘isn’, “isn’t”, ‘ma’, ‘mightn’, “mightn’t”, ‘mustn’, “mustn’t”, ‘needn’, “needn’t”, ‘shan’, “shan’t”, ‘shouldn’, “shouldn’t”, ‘wasn’, “wasn’t”, ‘weren’, “weren’t”, ‘won’, “won’t”, ‘wouldn’, “wouldn’t”]

        # Remove punctuation, numeric characters, convert to lowercase, and remove stopwords
        cleaned_text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
        cleaned_text = re.sub(r'\d+', '', cleaned_text)  # Remove numeric characters
        cleaned_text = cleaned_text.lower()  # Convert to lowercase
        cleaned_text = ' '.join([word for word in cleaned_text.split() if word not in stopwords])  # Remove stopwords
        return cleaned_text

    def mapper(self, _, line):
        for word in line.split():
            cleaned_word = self.clean_text(word)
            if cleaned_word:  # Check if the cleaned word is not empty
                yield (cleaned_word, 1)

    def reducer(self, word, counts):
        total_count = sum(counts)
        if total_count > 100:
            yield (word, total_count)

if __name__ == '__main__':
    MRWordCount.run()
```

# Perform and Output Job Result

1. run in local (easy to debug)
```
python3 mrjob_wordcount.py -r local /home/hadoop/workspace/4_title_reviews > wordcount_result.txt
```

2. run in HDFS (rating 4)
```
python3 mrjob_wordcount.py \
      -r hadoop  hdfs:///4_title_reviews > 4_mrjob_wordcount_result.txt
```

3. Look at time (local) (rating 4)
```
time python3 mrjob_wordcount.py -r local /home/hadoop/workspace/4_title_reviews > 4_mrjob_wordcount_result.txt
```

4. Look at time (hdfs) (rating 4)
```
time python3 mrjob_wordcount.py \
      -r hadoop  hdfs:///4_title_reviews > 4_mrjob_wordcount_result.txt
```

5. run in HDFS (rating 5)
```
python3 mrjob_wordcount.py \
      -r hadoop  hdfs:///5_title_reviews > 5_mrjob_wordcount_result.txt
```

7. Look at time (local) (rating 5)
```
time python3 mrjob_wordcount.py -r local /home/hadoop/workspace/5_title_reviews > 5_mrjob_wordcount_result.txt
```

8. Look at time (hdfs) (rating 5)
```
time python3 mrjob_wordcount.py \
      -r hadoop  hdfs:///5_title_reviews > 5_mrjob_wordcount_result.txt
```

# View Result (Hive and MrJob)

1. Start Hive Shell
```
hive
```

2. Create table (rating 4)
```
CREATE EXTERNAL TABLE IF NOT EXISTS 4_MrJob_wordcount (
  word STRING,
  count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/4_wordcount';
```
3. Move file to HDFS (cd workspace)
```
hdfs dfs -put /home/hadoop/workspace/4_mrjob_wordcount_result.txt /user/hive/4_mrjob_wordcount_result.txt
```

3. Insert data extracted from 4_mrjob_wordcount_result.txt (in hive shell)
```
LOAD DATA INPATH '/user/hive/4_mrjob_wordcount_result.txt' INTO TABLE 4_MrJob_wordcount;

```
4. View the highest repeated word (rating 4)
```
select * from 4_MrJob_wordcount order by count DESC limit 10;
```

6. Create table (rating 5)
```
CREATE EXTERNAL TABLE IF NOT EXISTS 5_MrJob_wordcount (
  word STRING,
  count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/5_wordcount';
```
7. Move file to HDFS (cd workspace)
```
hdfs dfs -put /home/hadoop/workspace/5_mrjob_wordcount_result.txt /user/hive/5_mrjob_wordcount_result.txt
```

8. Insert data extracted from 5_mrjob_wordcount_result.txt
```
LOAD DATA INPATH '/user/hive/5_mrjob_wordcount_result.txt' INTO TABLE 5_MrJob_wordcount;
```
9. View the highest repeated word (rating 5)
```
SELECT * from 5_MrJob_wordcount order by count DESC limit 10;
```
# Perform Wordcount using Mapreduce
1. Open new directory "java_wordcount"
```
mkdir java_wordcount
```
```
cd java_wordcount
```
2. Open "stubs" file in "java_wordcount" file
```
mkdir stubs
```
```
cd stubs
```

3.  open java file in "stubs"
```
nano SunReducer.java
```

Code inside SunReducer.java
```
code
```
```
nano WordCount.java
```

Code Inside WordCount.java
```
code
```
```
nano WordMapper.java
```

Code Inside WordMapper.java
```
code
```

4. compile 3 java classes
```
javac -classpath `hadoop classpath` stubs/*.java
```

5. Collect Java Files into a JAR file
```
jar cvf wc.jar stubs/*.class
```

6. Submit MapReduce Job to Hadoop using JAR File (rating 4)
```
hadoop jar wc.jar stubs.WordCount /4_title_reviews java_wordcount
```
7. Export to the current directory
```
hadoop fs -get java_wordcount/part-r-00000 /home/hadoop/workspace/4_java_wordcount.txt
```


3. Run java code on (rating 4)
4. Run jave code on (rating 5)
5. 


# Spark

Go into spark shell
```
pyspark
```

Spark py script (rating 1)
```
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

review4_rdd = sc.textFile("hdfs:///1_title_reviews")
review4_rdd = review4_rdd.map(lambda x: re.sub(r'\t', '', x))
review4_rdd = review4_rdd.map(lambda x: re.sub(r'[^\w\s]', '', x))

review4_rdd = review4_rdd.flatMap(lambda line: line.split(" "))
review4_rdd = review4_rdd.filter(lambda x: x != '')

# Remove stopwords using NLTK
stop_words = set(stopwords.words('english'))
review4_rdd = review4_rdd.filter(lambda x: x.lower() not in stop_words)

review4_count = review4_rdd.map(lambda word: (word, 1))
review4_wc = review4_count.reduceByKey(lambda x, y: (x + y))

review4_wc.saveAsTextFile('hdfs:///wc1spark')

# Show RunTime
end_time = time.time()
execution_time = end_time - start_time
print("Execution time: {:.2f} seconds".format(execution_time))

sc.stop()
```

py script (rating 5)
```
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
```

get .txt file from hdfs server (rating 4)
```
hadoop fs -get /cleaned_wc4spark/part-00000 /home/hadoop/workspace/cleaned_wc4spark.txt
```
```
hadoop fs -get /cleaned_wc4spark/part-00001 /home/hadoop/workspace/cleaned_wc4spark2.txt
```


get .txt file from hdfs server (rating 5)
```
hadoop fs -get /wc5spark/part-00000 /home/hadoop/workspace/wc5spark.txt
```
```
hadoop fs -get /wc5spark/part-00001 /home/hadoop/workspace/wc5spark1.txt
```

combine part-00000 and part-00001 (rating 4)
```
cat /home/hadoop/workspace/cleaned_wc4spark.txt /home/hadoop/workspace/cleaned_wc4spark2.txt > /home/hadoop/workspace/combined_wc4spark.txt
```

combine part-00000 and part-00001 (rating 5)
```
cat /home/hadoop/workspace/wc5spark.txt /home/hadoop/workspace/wc5spark1.txt > /home/hadoop/workspace/combined_wc5spark.txt
```

Create Wordcloud using R
```

```















