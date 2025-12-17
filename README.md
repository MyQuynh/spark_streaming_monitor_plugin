# Spark Streaming Plugin with Monitoring

A Spark Streaming application that processes Kafka messages with integrated monitoring.

## Architecture

```
Dummy Events → Kafka → Spark Streaming → Prometheus → Grafana
```

## Prerequisites

- Docker and Docker Compose
- Python 3.x with `kafka-python` package
- Java 8+ and sbt

## Setup Instructions

### 1. Start Services

```bash
cd deployment
docker-compose up -d
```

This starts Kafka (9092), Prometheus (9090), and Grafana (3000).

### 2. Generate Data

```bash
pip install kafka-python
cd deployment
python dummy_events.py
```

Sends 1000 dummy events to Kafka topic `dummy-topic`.

### 3. Run Spark Application

```bash
sbt compile
sbt run
```

Processes Kafka messages and exposes metrics for Prometheus.

## Monitoring

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)
  - Add Prometheus datasource: `http://prometheus:9090`

## Project Structure

```
├── build.sbt                    # Scala build config
├── src/main/scala/
│   ├── Application.scala        # Main Spark app
│   └── listeners/              # Custom metrics
├── deployment/
│   ├── docker-compose.yaml     # Infrastructure
│   └── dummy_events.py         # Data generator
```