# Real-Time Crypto Anomaly Detection Pipeline

A production-ready, event-driven microservice architecture designed to ingest real-time financial data (Bitcoin), stream it through an asynchronous message broker, detect market anomalies instantly using Machine Learning, and visualize the live results via WebSockets on a web dashboard.

## 🏗️ Architecture Overview

The system is fully decoupled and containerized into four core services working in harmony:

1. **Data Producer (Python / yfinance):** Fetches streaming, live 24/7 Bitcoin price and volume metrics from Yahoo Finance and publishes the payload instantly into a RabbitMQ message queue.
2. **Message Broker (RabbitMQ):** Acts as the asynchronous backbone of the system, implementing an event-driven architecture to ensure zero data loss between ingestion and analytical services.
3. **ML Anomaly Detector (Python / Scikit-Learn):** A consumer that processes incoming live ticks through a sliding time-window of the last 50 data points. It utilizes an **Isolation Forest** (Unsupervised Learning) algorithm to dynamically spot flash crashes, market pumps, or irregular volatility spikes. Detected anomalies are piped to the dashboard queue.
4. **Web Server & WebSocket Gateway (FastAPI):** Consumes the processed ML data asynchronously and broadcasts it in real-time over an active WebSocket tunnel to any connected frontend client.
5. **Live Console Dashboard (HTML5 / Chart.js):** A lightweight, low-latency UI that renders streaming crypto charts instantly, color-coding normal ticks vs anomalous spikes with an attached critical live alert log.

---

## 🛠️ Tech Stack

* **Language:** Python 3.11
* **Streaming & Event Pipelines:** RabbitMQ, Pika, Aio-Pika
* **Machine Learning:** Scikit-Learn (Isolation Forest), NumPy, Pandas
* **Web & Network Protocols:** FastAPI, Uvicorn, WebSockets
* **Data Sources:** Yahoo Finance API
* **DevOps & Infrastructure:** Docker, Docker Compose
* **Frontend Visualization:** Chart.js, Tailwind/Vanilla CSS

---

## 🚀 How to Run Locally

Since the entire ecosystem is orchestrated via Docker, you can spin up the full pipeline with a single command. 

### Prerequisites
Make sure you have [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running on your machine.

## 📂 Project Structure

├── Dockerfile # Setup for Python environments and dependencies

├── docker-compose.yml # Orchestration config for RabbitMQ, Producer, Consumer, Server

├── requirements.txt # Python external library specifications

├── producer.py # Financial data live ingestion script

├── consumer.py # Isolation Forest ML algorithm processing script

├── server.py # FastAPI application maintaining WebSocket tunnels

└── index.html # High-performance UI rendering Chart.js logic



### Execution
1. Clone this repository and navigate to the project directory:
   ```bash
   git clone <your-repository-url>
   cd realtime-crypto-anomaly-detection
