"""
Stock Market ETL Pipeline
Extracts stock data from Polygon API, transforms, and loads to Aiven PostgreSQL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, Integer, Date
from sqlalchemy.orm import declarative_base
import logging
import os

# Configure logging for debugging and monitoring
logger = logging.getLogger(__name__)
Base = declarative_base()

class StockData(Base):
    """Database model for storing stock market data"""
    __tablename__ = 'stock_market_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(10), nullable=False)  # Stock symbol
    date = Column(Date, nullable=False)          # Trading date
    open_price = Column(Float)                   # Opening price
    high_price = Column(Float)                   # Daily high
    low_price = Column(Float)                    # Daily low
    close_price = Column(Float)                  # Closing price
    volume = Column(Float)                       # Trading volume
    vwap = Column(Float)                         # Volume weighted average price
    transactions = Column(Integer)               # Number of transactions

def get_db_connection_string():
    """Build PostgreSQL connection string from environment variables"""
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    database = os.getenv('POSTGRES_DB')
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?sslmode=require"

def extract_stock_data(**context):
    """Extract stock market data from Polygon API for specified tickers"""
    logger.info("Starting data extraction")
    
    # Get API key from environment variables
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        raise ValueError("POLYGON_API_KEY not set")
    
    # Get execution date from Airflow context
    execution_date = context['logical_date']
    
    # Use recent trading day for testing if future date is provided
    if execution_date > datetime.now().replace(tzinfo=execution_date.tzinfo):
        execution_date = datetime(2024, 10, 18, tzinfo=execution_date.tzinfo)
    
    # Skip weekends (Saturday=5, Sunday=6) to get last trading day
    target_date = execution_date
    while target_date.weekday() >= 5:
        target_date -= timedelta(days=1)
    
    target_date_str = target_date.strftime('%Y-%m-%d')
    
    # List of stock tickers to fetch data for
    tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    extracted_data = []
    
    # Fetch data for each ticker from Polygon API
    for ticker in tickers:
        try:
            url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{target_date_str}/{target_date_str}"
            response = requests.get(url, params={'apiKey': api_key}, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            # Check if API returned valid data
            if data.get('resultsCount', 0) > 0 and 'results' in data:
                for result in data['results']:
                    extracted_data.append({
                        'ticker': ticker,
                        'timestamp': result.get('t'),        # Unix timestamp
                        'open': result.get('o'),            # Open price
                        'high': result.get('h'),            # High price
                        'low': result.get('l'),             # Low price
                        'close': result.get('c'),           # Close price
                        'volume': result.get('v'),          # Volume
                        'vwap': result.get('vw'),           # VWAP
                        'transactions': result.get('n')     # Number of transactions
                    })
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {ticker}: {str(e)}")
            continue
    
    logger.info(f"Extraction complete: {len(extracted_data)} records")
    return extracted_data if extracted_data else []

def transform_stock_data(**context):
    """Transform raw stock data into clean, structured format"""
    logger.info("Starting transformation")
    
    # Get data from previous task via XCom
    raw_data = context['ti'].xcom_pull(task_ids='extract_data')
    if not raw_data:
        return []
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(raw_data)
    
    # Convert Unix timestamp to date
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date
    
    # Rename columns to match database schema
    df = df.rename(columns={
        'open': 'open_price', 
        'high': 'high_price', 
        'low': 'low_price', 
        'close': 'close_price'
    })
    
    # Select only the columns we need for the database
    final_columns = ['ticker', 'date', 'open_price', 'high_price', 'low_price', 
                    'close_price', 'volume', 'vwap', 'transactions']
    
    logger.info(f"Transformation complete: {len(df)} records")
    return df[final_columns].to_dict('records')

def load_to_database(**context):
    """Load transformed data into PostgreSQL database"""
    logger.info("Starting data load")
    
    # Get transformed data from previous task
    transformed_data = context['ti'].xcom_pull(task_ids='transform_data')
    if not transformed_data:
        return
    
    # Convert to DataFrame for database insertion
    df = pd.DataFrame(transformed_data)
    
    # Create database connection
    engine = create_engine(get_db_connection_string(), echo=False)
    
    try:
        # Load data to database - replaces table if it exists
        df.to_sql(
            name='stock_market_data',
            con=engine,
            if_exists='replace',  # Drops and recreates table
            index=False           # Don't include DataFrame index
        )
        logger.info(f"✅ Successfully loaded {len(df)} records")
    except Exception as e:
        logger.error(f"❌ Database error: {str(e)}")
        raise
    finally:
        # Always close database connection
        engine.dispose()

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,      # Don't wait for previous runs
    'email_on_failure': False,     # Disable email alerts
    'retries': 3,                  # Retry failed tasks 3 times
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes between retries
}

# Define the DAG
dag = DAG(
    'stock_market_etl_pipeline',
    default_args=default_args,
    description='Stock market ETL pipeline - extracts, transforms, loads daily stock data',
    schedule='0 2 * * *',  # Run daily at 2 AM
    catchup=False,                  # Don't backfill past dates
    start_date=datetime(2024, 1, 1),  # Start date for the DAG
    tags=['etl', 'stock_market'],   # Tags for organization
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_stock_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_stock_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_to_database,
    dag=dag,
)

# Define task dependencies - run in sequence
extract_task >> transform_task >> load_task