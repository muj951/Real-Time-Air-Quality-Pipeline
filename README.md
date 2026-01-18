# Real-Time Distributed Air Quality Monitoring System

##  Project Description
This project implements an end-to-end Big Data pipeline for monitoring air quality (PM2.5, PM10) in real-time across global cities (Paris, Delhi, Beijing, etc.). 

The system fetches live sensor data from the **OpenAQ API**, ingests it using **Apache Kafka**, processes it with **Apache Spark Structured Streaming**, and persists it using a dual-sink strategy:
1. **HDFS (Parquet):** For long-term archival and batch analytics.
2. **PostgreSQL:** For low-latency querying and visualization.

Real-time dashboards are powered by **Grafana**, visualizing sensor locations, pollution trends, and cluster health metrics.

---

## Architecture & Cluster Setup
The project runs on a distributed cluster with the following node configuration:

| Node Role | IP Address | Service Roles |
|-----------|------------|---------------|
| **Master**| `10.0.0.40`| NameNode, ResourceManager, Spark Master, Kafka Broker, Zookeeper, Postgres |
| **Worker1**| `10.0.0.41`| DataNode, NodeManager, Spark Worker |
| **Worker2**| `10.0.0.42`| DataNode, NodeManager, Spark Worker |

### Prerequisites
* **OS:** Ubuntu 20.04/22.04 LTS
* **Java:** OpenJDK 8 or 11
* **Python:** 3.8+
* **Hadoop:** 3.3.x
* **Spark:** 3.5.x
* **Kafka:** 3.6.x
* **PostgreSQL:** 14+

---

## How to Run

### 1. Start Infrastructure Services
Ensure all distributed services are active on the Master node:
```bash
# Start Zookeeper & Kafka
$KAFKA_HOME/bin/zookeeper-server-start.sh config/zookeeper.properties
$KAFKA_HOME/bin/kafka-server-start.sh config/server.properties
```
# Start Hadoop (HDFS & YARN)
start-dfs.sh
start-yarn.sh

# Start Spark Cluster
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-workers.sh

2. Initialize Database
Create the required table in PostgreSQL:

psql -U postgres -d air_quality_db -f sql/schema.sql

3. Start the Pipeline
Step A: Start the Data Producer This script fetches data from OpenAQ and pushes it to the Kafka topic openaq_raw_readings.

python3 src/producer.py

Step B: Submit the Spark Job Submit the streaming processor to the Spark cluster. Ensure you include the Kafka and JDBC drivers.
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 \
  --master spark://10.0.0.40:7077 \
  src/spark_processor.py
  ```

Monitoring:

Grafana Dashboard: Access at http://localhost:3000

Geomap: Real-time sensor locations.

System Usage: CPU/RAM usage of Master vs. Worker nodes.

Spark UI: http://localhost:8080 (Job status & Application Logs)

Hadoop UI: http://localhost:9870 (HDFS storage verification)

Demo Video Link:

[🎬 Click Here to Watch the Demo Video](https://youtu.be/NPB28FMmr6I)

Github Repo Link:

[Click Here to go to git repo](https://github.com/muj951/Real-Time-Air-Quality-Pipeline)
