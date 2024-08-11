## Real-Time Twitter Sentiment Analysis Using Kafka, Spark (MLLib & Streaming), MongoDB.
---

#### Overview

This repository focused on real-time sentiment analysis of Twitter data (classification of tweets). The project leverages various technologies to collect, process and analyze data from tweets in real-time.

---
#### Project Architecture
The project is built using the following components:

- **Apache Kafka**: Used for real-time data ingestion from Twitter DataSet. (This is the data source: <a>https://www.kaggle.com/datasets/jp797498e/twitter-entity-sentiment-analysis</a>)

- **SparkML**: A machine learning library of PySpark used for training model to perform sentiment analysis

- **PySpark**: An Python API for Apache Spark processesing the streaming data from Kafka.

- **MongoDB**: Stores the processed sentiment data.
![architecture](/real-time-tw-sentiment/images/Architecture.png)
---

#### Features
- **Real-time Data Ingestion**: Collects live tweets using Kafka from the Twitter DataSet.
- **Stream Processing**: Utilizes Spark Streaming to process and analyze the data in real-time.
- **Sentiment Analysis**: Classifies tweets into different sentiment categories (negative, possitive, neutral, irrelevant) using natural language processing (NLP) techniques.
- **Data Storage**: Stores the sentiment analysis results in MongoDB for persistence.
---

#### Prerequisites
To run this project, you will need the following installed on your system (I use Window Operating System):

- Kafka
- Python 3.x 
- Apache Kafka
- Apache Spark (PySpark for python)
- MongoDB
---

#### Running the Project
1. Connect MongoDB at localhost:27017, create database **tweets_sentiment** and collection **tweets**
2. Start Kafka on your computer at kafka folder and create a topic named *tweets*:
   - **.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties**
   - **.\bin\windows\kafka-server-start.bat .\config\server.properties**
   - **.\bin\windows\kafka-topics.bat --create --topic tweets --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1**
3. Run producer.py file
4. Run consumer.py file

