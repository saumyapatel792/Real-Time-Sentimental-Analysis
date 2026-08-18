

#  Real-Time Sentiment Analysis

A real-time sentiment analysis system that processes streaming text data using **Apache Kafka** and **Apache Spark**, applies a machine learning sentiment classification model, and presents the analysis through an interactive dashboard.

The project demonstrates an end-to-end **real-time Big Data + Machine Learning pipeline** for analyzing sentiment from incoming text data.

---

##  Project Overview

The system is designed to continuously receive text data, process it in real time, classify the sentiment of each text, and generate results that can be visualized through a dashboard.

###  High-Level Workflow

```text
                    ┌─────────────────────┐
                    │   Input / Dataset   │
                    │   Sentiment Data    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Apache Kafka      │
                    │   Data Streaming    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Apache Spark      │
                    │  Stream Processing  │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Sentiment ML Model  │
                    │ Positive / Negative │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │       Output        │
                    │ Processed Results   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     Dashboard       │
                    │ Visualization       │
                    └─────────────────────┘
```

---

##  Key Features

*  Real-time text stream processing
*  Apache Kafka-based message streaming
*  Apache Spark stream processing
*  Machine Learning-based sentiment classification
*  Interactive sentiment visualization
*  Structured output generation
*  Modular project architecture
*  Suitable for large-scale text processing
*  Separate components for streaming, ML, processing, and visualization

---

##  Technologies Used

| Technology           | Purpose                                |
| -------------------- | -------------------------------------- |
| **Python**           | Application and ML development         |
| **Apache Kafka**     | Real-time data streaming               |
| **Apache Spark**     | Distributed stream processing          |
| **PySpark**          | Spark integration with Python          |
| **Machine Learning** | Sentiment classification               |
| **Dashboard**        | Result visualization                   |
| **Git/GitHub**       | Version control and project management |

---

##  Project Structure

```text
RealTimeSentimentAnalysis/
│
├── dashboard/
│   └── # Dashboard and visualization components
│
├── data/
│   └── # Dataset and input data
│
├── kafka/
│   └── # Kafka producer/consumer and streaming configuration
│
├── models/
│   └── # Trained sentiment analysis models
│
├── output/
│   └── # Generated sentiment analysis results
│
├── spark/
│   └── # Apache Spark processing and streaming code
│
├── load_data.py
│   └── # Data loading / streaming utility
│
├── .gitignore
│
└── README.md
```

> **Note:** Large dataset files are intentionally excluded from this repository because GitHub has a 100 MB per-file limit. The dataset can be downloaded separately and placed inside the `data/` directory.

---

#  Sentiment Analysis

Sentiment analysis is a Natural Language Processing (NLP) task used to determine the emotional polarity of text.

For example:

| Input Text                        | Sentiment   |
| --------------------------------- | ----------- |
| "This movie was fantastic!"       | 😊 Positive |
| "I really enjoyed this product."  | 😊 Positive |
| "The movie was boring."           | 😞 Negative |
| "This was a terrible experience." | 😞 Negative |

The system processes these texts continuously rather than analyzing the complete dataset as a single batch.

---

#  Apache Kafka

Apache Kafka is used as the **real-time messaging and streaming layer**.

The basic flow is:

```text
Producer
   │
   │ Text Messages
   ▼
Kafka Topic
   │
   │ Stream
   ▼
Consumer / Spark
```

### Kafka Responsibilities

* Receive incoming text
* Store messages in Kafka topics
* Provide real-time message streaming
* Allow Spark to consume incoming messages

---

#  Apache Spark

Apache Spark is responsible for processing the incoming streaming data.

The Spark processing pipeline can be represented as:

```text
Kafka Stream
     │
     ▼
Spark Structured Streaming
     │
     ▼
Text Preprocessing
     │
     ▼
Feature Processing
     │
     ▼
Sentiment Model
     │
     ▼
Prediction
     │
     ▼
Output
```

Spark provides scalable processing capabilities and is suitable for handling large volumes of streaming text.

---

#  Machine Learning Pipeline

The sentiment analysis component follows a typical NLP machine learning workflow:

```text
Raw Text
   │
   ▼
Text Preprocessing
   │
   ▼
Feature Extraction
   │
   ▼
Trained ML Model
   │
   ▼
Sentiment Prediction
   │
   ├── Positive
   │
   └── Negative
```

The trained models are stored inside the:

```text
models/
```

directory.

---

#  Dashboard

The `dashboard/` directory contains the visualization component of the project.

The dashboard is intended to provide an easy-to-understand representation of the real-time sentiment results.

Possible metrics include:

* Total messages analyzed
* Positive sentiment count
* Negative sentiment count
* Sentiment distribution
* Real-time prediction results
* Recent analyzed messages

Example:

```text
┌─────────────────────────────────────────────┐
│        REAL-TIME SENTIMENT ANALYSIS         │
├─────────────────────────────────────────────┤
│                                             │
│   Total Reviews        Positive             │
│       1250                820               │
│                                             │
│   Negative             Sentiment            │
│       430              Distribution         │
│                                             │
│        📈 Real-Time Sentiment Chart         │
│                                             │
└─────────────────────────────────────────────┘
```

---

#  Dataset

The project uses sentiment-labelled text data for training/testing and streaming experiments.

Large dataset files are **not included in the GitHub repository** because of GitHub's file-size restrictions.

After downloading the dataset, place the required files inside:

```text
data/
```

For example:

```text
data/
├── trainingandtestdata.zip
└── training.1600000.processed.noemoticon.csv
```

These files are kept locally and are excluded using `.gitignore`.

---

#  Installation

## 1. Clone the Repository

```bash
git clone https://github.com/saumyapatel792/Real-Time-Sentimental-Analysis.git
```

Move into the project:

```bash
cd Real-Time-Sentimental-Analysis
```

---

## 2. Create a Python Virtual Environment

Linux / WSL:

```bash
python3 -m venv venv
```

Activate it:

```bash
source venv/bin/activate
```

Windows:

```powershell
python -m venv venv
```

Activate:

```powershell
venv\Scripts\activate
```

---

## 3. Install Dependencies

If a `requirements.txt` file is available in the project:

```bash
pip install -r requirements.txt
```

Otherwise, install the required packages used by the individual project components.

---

#  Kafka Setup

Make sure Apache Kafka and its required services are running.

Verify Kafka installation:

```bash
kafka-topics.sh --version
```

Create a topic for sentiment data:

```bash
kafka-topics.sh --create \
  --topic sentiment \
  --bootstrap-server localhost:9092
```

List topics:

```bash
kafka-topics.sh --list \
  --bootstrap-server localhost:9092
```

> Adjust the Kafka commands according to your installed Kafka version and configuration.

---

#  Spark Setup

Verify Spark:

```bash
spark-submit --version
```

or:

```bash
pyspark --version
```

The Spark component consumes streaming messages from Kafka and performs sentiment processing.

---

#  Running the Project

The project can be executed as a pipeline:

```text
1. Start Kafka
       ↓
2. Start Kafka topic
       ↓
3. Start data producer
       ↓
4. Start Spark streaming
       ↓
5. Apply sentiment model
       ↓
6. Generate output
       ↓
7. Start dashboard
```

### Start Data Loading

From the project root:

```bash
python load_data.py
```

### Start Spark Processing

Navigate to the Spark directory:

```bash
cd spark
```

Run the appropriate Spark application:

```bash
spark-submit <spark_application>.py
```

Replace `<spark_application>.py` with the Spark streaming script in the project.

### Start Dashboard

Navigate to:

```bash
cd dashboard
```

Run the dashboard application according to the dashboard framework used in the project.

---

#  Example Real-Time Processing

An incoming message might look like:

```text
"I absolutely loved this movie!"
```

The system processes it as:

```text
Incoming Text
      ↓
Kafka
      ↓
Spark Streaming
      ↓
Text Preprocessing
      ↓
ML Model
      ↓
Positive
      ↓
Dashboard
```

Another message:

```text
"The movie was extremely boring."
```

could produce:

```text
Sentiment: Negative
```

---

#  Output

Processed results are stored in:

```text
output/
```

The output can be used for:

* Sentiment statistics
* Dashboard visualization
* Analysis
* Monitoring
* Further machine learning experiments

---

#  Use Cases

This architecture can be adapted for several real-world applications:

*  Movie review analysis
*  Product review monitoring
*  Social media sentiment analysis
*  News sentiment analysis
*  Customer feedback analysis
*  Customer service monitoring
*  Brand reputation monitoring

---

#  Advantages

### Real-Time Processing

Kafka allows incoming data to be streamed continuously instead of waiting for a complete dataset.

### Scalable Processing

Spark provides distributed processing capabilities for large-scale data.

### Machine Learning Integration

The streaming pipeline can directly connect incoming text with a trained sentiment model.

### Modular Architecture

Each component is separated:

```text
Kafka → Spark → ML Model → Output → Dashboard
```

This makes the system easier to maintain and extend.

---

#  Future Enhancements

Potential improvements include:

* [ ] Add positive, negative, and neutral sentiment classes
* [ ] Add advanced NLP transformer models
* [ ] Add real-time sentiment alerts
* [ ] Add Kafka monitoring
* [ ] Add Spark monitoring
* [ ] Add Docker containerization
* [ ] Add MLflow experiment tracking
* [ ] Add Prometheus and Grafana monitoring
* [ ] Deploy the application to the cloud
* [ ] Add automated CI/CD
* [ ] Add model performance monitoring
* [ ] Add multilingual sentiment analysis

---

#  Learning Outcomes

This project demonstrates practical experience with:

* Big Data processing
* Real-time data streaming
* Apache Kafka
* Apache Spark
* Spark Structured Streaming
* Natural Language Processing
* Machine Learning
* Python
* Data preprocessing
* Real-time visualization
* Git and GitHub

---

#  Author

**Saumya Patel**

Data Science / Machine Learning Enthusiast

GitHub:
https://github.com/saumyapatel792

Project Repository:
https://github.com/saumyapatel792/Real-Time-Sentimental-Analysis

---

---

##  Support

If you find this project useful, consider giving the repository a ⭐ on GitHub.
