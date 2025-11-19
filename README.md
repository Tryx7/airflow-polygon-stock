# Data Engineering Pipeline Project

A comprehensive data engineering platform featuring real-time streaming, batch ETL pipelines, and data processing with Apache Airflow, Spark, Kafka, and PostgreSQL.

## 🏗️ Architecture Overview

```
Data Sources → Airflow ETL → Processing → Storage → Analytics
    ↓              ↓           ↓           ↓         ↓
 YouTube API    Spark       Kafka      PostgreSQL  Grafana
 Polygon API    PySpark     Cassandra   MongoDB
```

## 📁 Project Structure

```
├── dags/
│   └── stock_market_dag.py          # Stock market ETL pipeline
├── scripts/
│   └── spark_processing.py          # Spark data processing
├── docker-compose.yaml              # Multi-service infrastructure
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment template
└── data/                           # Data directories
    ├── raw/                        # Raw JSON data
    └── processed/                  # Processed Parquet files
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- API Keys for YouTube and Polygon APIs

### 1. Environment Setup

```bash
# Copy environment template
cp .env.example .env

# Edit with your credentials
nano .env
```

### 2. Start Infrastructure

```bash
# Build and start all services
docker-compose up -d --build

# Check service status
docker-compose ps
```

### 3. Initialize Airflow

```bash
# Create admin user (first time only)
docker-compose exec airflow-apiserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 4. Access Services

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Grafana**: http://localhost:3000 (admin/admin)
- **MongoDB**: localhost:27017
- **Kafka**: localhost:9092
- **Cassandra**: localhost:9042

## 🔧 Core Components

### 1. Stock Market ETL Pipeline (`stock_market_dag.py`)

**Purpose**: Daily extraction of stock market data from Polygon API

**Features**:
- Extracts data for major stocks (AAPL, GOOGL, MSFT, AMZN, TSLA)
- Transforms and cleans financial data
- Loads to Aiven PostgreSQL with schema enforcement
- Automated daily execution at 2 AM

**Data Schema**:
```sql
ticker | date | open_price | high_price | low_price | close_price | volume | vwap | transactions
```

### 2. Spark Data Processing (`spark_processing.py`)

**Purpose**: Process YouTube analytics data using PySpark

**Features**:
- Channel statistics processing (subscribers, views, video count)
- Video analytics (engagement rates, publishing patterns)
- Data transformation and enrichment
- Output to PostgreSQL and Parquet formats

**Output Tables**:
- `channel_stats`: Channel-level metrics
- `video_stats`: Video-level analytics with engagement rates

### 3. Data Infrastructure

**Database Services**:
- **PostgreSQL**: Primary relational data store
- **MongoDB**: Document storage for unstructured data
- **Cassandra**: Time-series and high-write workloads

**Streaming & Messaging**:
- **Kafka**: Real-time data streaming
- **Zookeeper**: Kafka coordination

**Monitoring**:
- **Grafana**: Data visualization and dashboards
- **Airflow**: Workflow orchestration and monitoring

## ⚙️ Configuration

### Environment Variables

Create `.env` file with:

```env
# Airflow
AIRFLOW_UID=1000
AIRFLOW_IMAGE_NAME=custom-airflow:pyspark

# YouTube API
YOUTUBE_API_KEY=your_youtube_api_key
YOUTUBE_CHANNEL_ID=your_channel_id

# Polygon API
POLYGON_API_KEY=your_polygon_api_key

# Aiven PostgreSQL
POSTGRES_USER=your_db_user
POSTGRES_PASSWORD=your_db_password
POSTGRES_DB=your_database
POSTGRES_HOST=your_host.aivencloud.com
POSTGRES_PORT=12345
```

### API Keys Required

1. **YouTube Data API v3**: From Google Cloud Console
2. **Polygon.io API**: For stock market data
3. **Aiven PostgreSQL**: Cloud database credentials

## 📊 Data Pipelines

### Stock Market Pipeline
```
Extract → Transform → Load
   ↓         ↓         ↓
Polygon → PySpark → PostgreSQL
  API              Data Warehouse
```

### YouTube Analytics Pipeline
```
Extract → Spark → PostgreSQL → Analytics
   ↓              Processing     ↓
YouTube API                   Grafana Dashboards
```

## 🛠️ Development

### Running Spark Jobs

```bash
# Submit Spark job
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/airflow/scripts/spark_processing.py
```

### Manual DAG Trigger

```bash
# Trigger stock market ETL
docker-compose exec airflow-apiserver airflow dags trigger stock_market_etl_pipeline
```

### Database Connections

```python
# PostgreSQL Connection
conn_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?sslmode=require"

# MongoDB Connection
from pymongo import MongoClient
client = MongoClient('mongodb://admin:password@mongodb:27017/')
```

## 📈 Monitoring & Operations

### Airflow Dashboard
- Monitor DAG execution and task status
- View execution logs and retry failed tasks
- Manage variables and connections

### Grafana Analytics
- Create dashboards for stock performance
- Monitor data pipeline health
- Visualize YouTube channel metrics

### Service Health Checks
```bash
# Check all services
docker-compose ps

# View logs
docker-compose logs airflow-scheduler
docker-compose logs kafka
```

## 🔄 Extending the Project

### Adding New Data Sources
1. Create new DAG in `dags/` directory
2. Add API credentials to `.env`
3. Define data model and transformation logic
4. Update `docker-compose.yaml` if new services needed

### Custom Spark Jobs
1. Add script to `scripts/` directory
2. Define Spark session and processing logic
3. Add to Airflow DAG or run independently

### New Database Storage
1. Add service to `docker-compose.yaml`
2. Create connection in Airflow
3. Update Spark/Python scripts with new connection

## 🐛 Troubleshooting

### Common Issues

**Airflow DAG not appearing**:
- Check DAG file is in `dags/` directory
- Verify DAG has no syntax errors
- Restart airflow-scheduler service

**Database connection failures**:
- Verify credentials in `.env` file
- Check network connectivity to Aiven
- Confirm SSL certificates are trusted

**Spark job failures**:
- Check Python dependencies in `requirements.txt`
- Verify data file paths exist
- Check Spark executor memory settings

### Logs and Debugging

```bash
# View specific service logs
docker-compose logs airflow-scheduler
docker-compose logs kafka

# Debug DAG execution
docker-compose exec airflow-apiserver airflow tasks list stock_market_etl_pipeline

# Check database connectivity
docker-compose exec postgres psql -U airflow -d airflow
```

## 📝 License

This project is for educational and demonstration purposes. Adapt and extend for your specific use cases.

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am 'Add new feature'`)
4. Push to branch (`git push origin feature/improvement`)
5. Create Pull Request

---

**Note**: Replace all placeholder credentials in `.env` with your actual API keys and database credentials before running the project.
