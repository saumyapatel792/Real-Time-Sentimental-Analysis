# Real-Time Sentiment Analysis Big Data Pipeline

A real-time sentiment analysis system that processes streaming text data using **Apache Kafka** and **Apache Spark**, applies a machine learning sentiment classification model, and presents the analysis through an interactive live dashboard.

```
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

## 🚀 How to Run in WSL (Ubuntu)

### Option 1: One-Command Full Pipeline Startup
```bash
cd /mnt/c/BigData/RealTimeSentimentAnalysis
chmod +x run_pipeline_wsl.sh
./run_pipeline_wsl.sh
```

### Option 2: Step-by-Step Execution in WSL

#### Step 1: Start Apache Kafka (Terminal 1)
```bash
# If using KRaft or Zookeeper in WSL
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
```

#### Step 2: Start the Real-Time Dashboard Server (Terminal 2)
```bash
cd /mnt/c/BigData/RealTimeSentimentAnalysis
python3 dashboard/server.py
```
👉 Open your browser at **`http://localhost:8050`**

#### Step 3: Run PySpark Streaming Engine (Terminal 3)
```bash
cd /mnt/c/BigData/RealTimeSentimentAnalysis
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark/spark_streaming.py
```

#### Step 4: Run Kafka Sentiment Producer (Terminal 4)
```bash
cd /mnt/c/BigData/RealTimeSentimentAnalysis
python3 kafka/producer.py 2.0
```

---

## 🌐 Live Web Dashboard Features

Open **[http://localhost:8050](http://localhost:8050)** to access:
- **Interactive Testing Console**: Click sample chips or type custom text to see instant classification.
- **Pipeline Architecture Visualizer**: Live pulsating indicators tracking packets across the 6 pipeline stages.
- **Live Stream Feed**: Incoming messages classified in real time with emoji badges (`😊 Positive`, `😞 Negative`, `😐 Neutral`), confidence scores, and polarity meters.
- **Dynamic Visual Analytics**: Real-time Donut Chart for Sentiment Breakdown, Polarity Dynamics Sparkline timeline, and Top Keywords Frequency.
- **Stream Controls**: Play, Pause, Adjust stream speed (0.5s / 2.0s / 4.0s), and Clear feed.

---

## 🧪 Sentiment Analysis Examples

| Input Text | Sentiment | Polarity | Confidence |
| :--- | :--- | :--- | :--- |
| "This movie was fantastic!" | 😊 Positive | `+0.655` | `90.5%` |
| "I really enjoyed this product." | 😊 Positive | `+0.675` | `91.2%` |
| "The movie was boring." | 😞 Negative | `-0.630` | `89.5%` |
| "This was a terrible experience." | 😞 Negative | `-0.655` | `90.5%` |

---

## 🛠️ Project Structure

```text
RealTimeSentimentAnalysis/
│
├── dashboard/
│   ├── server.py              # FastAPI + WebSockets + Real-time Streaming Hub
│   └── static/
│       ├── index.html         # Live dashboard visualization UI
│       ├── style.css          # Dark-theme glassmorphism design system
│       └── app.js             # Real-time WebSocket & Chart.js engine
│
├── data/                      # Dataset files (excluded from Git)
│
├── kafka/
│   ├── producer.py            # Real-time streaming Kafka producer
│   └── tweets.txt             # Sample streaming sentences
│
├── models/                    # Trained sentiment classification models
│
├── spark/
│   ├── sentiment_engine.py    # NLP Polarity scoring & confidence engine
│   ├── spark_streaming.py     # PySpark Structured Streaming pipeline
│   └── train_model.py         # PySpark MLlib training script
│
├── run_pipeline_wsl.sh        # Complete WSL startup automation script
├── test_pipeline.py           # Automated test & verification script
└── README.md
```

---

## 👤 Author

**Saumya Patel**  
GitHub: [https://github.com/saumyapatel792](https://github.com/saumyapatel792)  
Repository: [https://github.com/saumyapatel792/Real-Time-Sentimental-Analysis](https://github.com/saumyapatel792/Real-Time-Sentimental-Analysis)
