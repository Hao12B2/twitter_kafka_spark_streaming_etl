'''
#from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'tweets',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit='True',
    #group_id='mygroup',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)
'''
import re
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import json
from pymongo import MongoClient

# Establish connection to MongoDB
client = MongoClient('localhost', 27017)
database = client['tweets_sentiment']
collection = database['tweets']

# Create SparkSession
spark = SparkSession.builder.appName("Tweets Sentiment with PySpark").getOrCreate()

# Load model
pipeline = PipelineModel.load('..\\model_sparkml\\logistic_regression_model.pkl')

df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'tweets') \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

def extract_last_value(value):
    try:
        data = json.loads(value)
        return data[-1] if isinstance(data, list) else value
    except json.JSONDecodeError:
        return value

extract_last_value_udf = udf(extract_last_value, StringType())
df = df.withColumn("tweet", extract_last_value_udf(col("value")))

def clean_text(text):
    if text is not None:
        text = re.sub(r'https?://\S+|www\.\S+|\.com\S+|youtu\.be/\S+', '', text)
        text = re.sub(r'(@|#)\w+', '', text)
        text = text.lower()
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    else:
        return ''

clean_text_udf = udf(clean_text, StringType())
df = df.withColumn("text", clean_text_udf(col("tweet")))

class_index_mapping = {0: 'Negative', 1: 'Positive', 2: 'Neutral', 3: 'Irrelevant'}

def process_batch(batch_df, batch_id):
    # Create SparkSession inside the function
    spark = SparkSession.builder.getOrCreate()
    
    batch_df = batch_df.withColumn("text", clean_text_udf(col("tweet")))
    processed_validation = pipeline.transform(batch_df)
    predictions = processed_validation.select("tweet", "text", "prediction").collect()
    
    for row in predictions:
        tweet = row['tweet']
        preprocessed_tweet = row['text']
        prediction = row['prediction']
        prediction_classname = class_index_mapping[int(prediction)]
        
        print("-> Tweet:", tweet)
        print("-> preprocessed_tweet:", preprocessed_tweet)
        print("-> Predicted Sentiment:", prediction)
        print("-> Predicted Sentiment classname:", prediction_classname)
        
        tweet_doc = {
            "tweet": tweet,
            "prediction": prediction_classname
        }
        collection.insert_one(tweet_doc)

query = df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode('append') \
    .start()

query.awaitTermination()

spark.stop()

