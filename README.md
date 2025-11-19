# Kafka Employee Data Streaming Project

A complete Kafka-based data streaming project that generates, produces, and consumes employee records in real-time.

## 📋 Project Overview

This project demonstrates a complete data streaming pipeline using Apache Kafka:
- **Producer**: Generates fake employee data and sends it to a Kafka topic
- **Consumer**: Reads employee records from the Kafka topic in real-time
- **Data Generator**: Creates realistic employee data using Faker library
- **Infrastructure**: Docker-based setup with Kafka, Zookeeper, Kafdrop, and Grafana

## 🏗️ Architecture

```
Data Generator → Kafka Producer → Kafka Topic → Kafka Consumer
                    ↓
           Kafka Cluster (Docker)
                    ↓
        Kafdrop UI + Grafana Dashboard
```

## 📁 Project Structure

```
├── consumer.py          # Kafka consumer application
├── producer.py          # Kafka producer application  
├── data_gen.py          # Fake employee data generator
├── docker-compose.yml   # Infrastructure configuration
└── README.md           # This file
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.7+
- Required Python packages: `kafka-python`, `faker`

### 1. Start Infrastructure

```bash
docker compose up -d
```

This starts:
- **Zookeeper** (port 2181)
- **Kafka Broker** (ports 9092, 9094)
- **Kafdrop UI** (port 9000) - Web UI for Kafka topic management
- **Grafana** (port 3000) - Monitoring dashboard

### 2. Install Python Dependencies

```bash
pip install kafka-python faker
```

### 3. Run the Producer

```bash
python producer.py
```

This will generate 4 employee records and send them to the `employee` topic.

### 4. Run the Consumer

```bash
python consumer.py
```

The consumer will start listening for messages on the `employee` topic and display them in real-time.

## 🔧 Configuration

### Kafka Topics
- **Topic Name**: `employee`
- **Consumer Group**: `employee-consumer-group`
- **Auto Offset Reset**: `earliest` (reads from beginning)

### Data Schema
Each employee record contains:
```json
{
  "name": "John Doe",
  "email": "john.doe@email.com", 
  "job": "Software Engineer",
  "salary": 85000.50
}
```

## 🌐 Web Interfaces

### Kafdrop UI
- **URL**: http://localhost:9000
- **Purpose**: Monitor Kafka topics, partitions, offsets, and messages
- **Features**: Browse topics, view message payloads, monitor consumer groups

### Grafana Dashboard  
- **URL**: http://localhost:3000
- **Purpose**: Visualize Kafka metrics and streaming data
- **Default Credentials**: admin/admin

## 📊 Usage Examples

### Produce Messages
```python
from producer import main as produce_messages
produce_messages()
```

### Consume Messages  
```python
from consumer import main as consume_messages
consume_messages()
```

### Generate Sample Data
```python
from data_gen import gen
employees = gen(5)  # Generate 5 employee records
```

## 🛠️ Troubleshooting

### Common Issues

1. **Connection Errors**: Ensure Docker containers are running
   ```bash
   docker compose ps
   ```

2. **Port Conflicts**: Check if ports 9092, 9094, 9000, 3000 are available

3. **Python Dependencies**: Verify all packages are installed
   ```bash
   pip list | grep -E "kafka-python|faker"
   ```

4. **Kafka Not Ready**: Wait 30-60 seconds after starting containers for Kafka to initialize

### Reset Environment
```bash
# Stop and remove containers
docker compose down

# Clean volumes (removes all data)
docker compose down -v

# Restart fresh
docker compose up -d
```

## 📈 Monitoring

- Use Kafdrop to monitor topic health and message flow
- Use Grafana for advanced metrics and visualization
- Check consumer offsets and lag in Kafdrop UI

## 🔄 Extending the Project

- Add more fields to employee data in `data_gen.py`
- Implement multiple consumers with different group IDs
- Add Avro serialization for better schema management
- Integrate with databases for persistent storage
- Add error handling and retry mechanisms

## 📝 License

This project is for educational purposes. Feel free to modify and extend for your use cases.

## 🤝 Contributing

1. Fork the project
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request
