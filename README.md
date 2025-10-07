# 🚀 Flink E-Commerce Real-Time Streaming Pipeline

This repository contains an **E-Commerce real-time streaming pipeline** built using **Apache Flink**, **Kafka**, **PostgreSQL**, and **Elasticsearch**, with **Kibana** for live data visualization.  
The project demonstrates **real-time stream processing**, **data aggregation**, and **multi-sink storage** (PostgreSQL and Elasticsearch).

---

## 🧩 Components

- **Apache Flink** — for stream processing and aggregations.  
- **Kafka** — message broker for real-time event streaming.  
- **PostgreSQL** — for storing transactional and aggregated data.  
- **Elasticsearch** — for fast search and analytics.  
- **Kibana** — for visualizing live data from Elasticsearch.  
- **Docker & Docker Compose** — to containerize and orchestrate all components.  
- **IntelliJ IDEA** — for writing and building the Flink job with Maven support.

---

## 🗂️ Code Structure

```
Flink_E-commerce/
│
├── src/
│   └── main/java/FlinkCommerce/
│       ├── DataStreamJob.java        # Main Flink application logic
│       ├── deserializer/             # Deserialization logic for Kafka messages
│       ├── dto/                      # Data Transfer Object (DTO) classes
│       └── utils/                    # Utility and JSON conversion classes
│
├── docker-compose.yml                # Defines services: Postgres, Kafka, Elasticsearch, Kibana
├── requirements.txt                  # Python dependencies for Kafka producer
├── main.py                           # Kafka producer for sending E-commerce data
└── target/FlinkCommerce-1.0-SNAPSHOT.jar  # Compiled Flink job JAR
```

---

## ⚙️ Flink Configuration Overview

- **Kafka Source**:  
  - Reads real-time transaction data from the Kafka topic.
  - Configurations include:
    - `bootstrap.servers`
    - `topic`
    - `group.id`

- **Flink Job**:
  - Performs stream processing and real-time aggregations.
  - Generates the following tables in PostgreSQL:
    - `transactions`
    - `sales_per_category`
    - `sales_per_day`
    - `sales_per_month`
    - `category_performance`
    - `discount_analysis`
    - `gift_order_metrics`
    - `sales_per_device`
    - `shipping_analysis`
    - `order_status_metrics`

- **Sinks**:
  - **PostgreSQL Sink (via JDBC)** — for structured data analysis and reporting.
  - **Elasticsearch Sink** — for real-time search and visualization in Kibana.



---

## 🧱 Setting Up the Environment

### 1️⃣ Install Apache Flink (v1.18.0)

```bash
cd /usr/local
sudo wget https://archive.apache.org/dist/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz
sudo tar -xzf flink-1.18.0-bin-scala_2.12.tgz
sudo ln -s flink-1.18.0 flink
```

Add Flink to your environment variables:

```bash
echo 'export FLINK_HOME=/usr/local/flink' >> ~/.bashrc
echo 'export PATH=$PATH:$FLINK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

Verify installation:

```bash
flink --version
```

You should see:
```
Version: 1.18.0
```

---

### 2️⃣ Configure Flink

Navigate to the configuration directory:

```bash
cd /usr/local/flink/conf
nano flink-conf.yaml
```

Ensure the following configurations are set:

```yaml
jobmanager.rpc.address: localhost
jobmanager.rpc.port: 6123
jobmanager.bind-host: localhost
jobmanager.memory.process.size: 1600m

taskmanager.bind-host: localhost
taskmanager.host: localhost
taskmanager.memory.process.size: 1728m
taskmanager.numberOfTaskSlots: 4

parallelism.default: 2

rest.address: localhost
rest.bind.address: localhost
```

> 💡 If using **WSL** and you want to open the Flink web UI from Windows,  
> change both `rest.address` and `rest.bind.address` to `0.0.0.0`.

Start the Flink cluster:

```bash
/usr/local/flink/bin/start-cluster.sh
```

Access the Flink Dashboard at:  
👉 [http://localhost:8081](http://localhost:8081)

---

### 3️⃣ Start the Docker Environment

In the project root directory:

```bash
docker compose up -d
```

This will start the containers for:
- PostgreSQL  
- Kafka  
- Elasticsearch  
- Kibana

---

### 4️⃣ Set Up and Run the Kafka Producer

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the producer to start streaming data to Kafka:

```bash
python main.py
```

---

### 5️⃣ Run the Flink Streaming Job

Make sure you’re in the directory containing the compiled JAR file (`target/FlinkCommerce-1.0-SNAPSHOT.jar`).

Run the job using:

```bash
/usr/local/flink/bin/flink run -c FlinkCommerce.DataStreamJob target/FlinkCommerce-1.0-SNAPSHOT.jar
```

---

## 🧾 Verify Data Ingestion

### 🐘 PostgreSQL

Enter the PostgreSQL interactive terminal:

```bash
docker exec -it postgres psql -U postgres -d postgres
```

List all created tables:

```sql
\d
```
![alt text](image-2.png)

Check data in one of the aggregated tables:

```sql
SELECT * FROM shipping_analysis;
```

---

### 🔍 Elasticsearch & Kibana

Open Kibana at:  
👉 [http://localhost:5601](http://localhost:5601)

Navigate to:  
**Management → Dev Tools**

Run the following query:

```json
GET /transactions/_search
```

You should see a portion of your transaction data indexed in Elasticsearch.

Then go to:  
**Analytics → Dashboard**  
and start building your visualizations! 🎨

---

## ✅ Summary

This project showcases how to build a **real-time data streaming pipeline** with:
- **Flink** for stream processing and aggregation  
- **Kafka** for real-time data ingestion  
- **PostgreSQL** and **Elasticsearch** as dual sinks  
- **Kibana** for live data analytics and visualization  
