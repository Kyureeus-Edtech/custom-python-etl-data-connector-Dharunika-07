import os
import requests
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class ETLExtractor:
    """Handles data extraction from the API"""
    
    def __init__(self, base_url):
        self.base_url = base_url
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
    def fetch_data(self, endpoint, params=None):
        """Fetch data from API endpoint"""
        url = f"{self.base_url}/{endpoint}"
        try:
            logger.info(f"Fetching data from: {url}")
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data: {e}")
            return None

class ETLTransformer:
    """Handles data transformation"""
    
    @staticmethod
    def transform_posts(posts_data):
        """Transform posts data for MongoDB"""
        if not posts_data:
            return None
            
        transformed = []
        current_time = datetime.utcnow()
        
        for post in posts_data:
            transformed_post = {
                'post_id': post['id'],
                'user_id': post['userId'],
                'title': post['title'],
                'body': post['body'],
                'ingestion_timestamp': current_time
            }
            transformed.append(transformed_post)
            
        return transformed

class ETLLoader:
    """Handles data loading to MongoDB"""
    
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = None
        
    def __enter__(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri)
            logger.info("Connected to MongoDB successfully")
            return self
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            raise
            
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
            
    def load_data(self, collection_name, data):
        """Load data into MongoDB collection"""
        if not data:
            logger.warning("No data to load")
            return False
            
        try:
            db = self.client[self.db_name]
            collection = db[collection_name]
            result = collection.insert_many(data)
            logger.info(f"Inserted {len(result.inserted_ids)} documents into {collection_name}")
            return True
        except Exception as e:
            logger.error(f"Error loading data to MongoDB: {e}")
            return False

def run_etl_pipeline():
    """Run the complete ETL pipeline"""
    
    API_BASE_URL = "https://jsonplaceholder.typicode.com"
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    DB_NAME = "etl_data"
    COLLECTION_NAME = "posts_raw"
    
    extractor = ETLExtractor(API_BASE_URL)
    transformer = ETLTransformer()
    
    # Extract data
    logger.info("Starting ETL pipeline - Extract phase")
    posts_data = extractor.fetch_data("posts")
    
    if not posts_data:
        logger.error("Failed to extract data from API")
        return False
    
    # Transform data
    logger.info("Transforming data")
    transformed_data = transformer.transform_posts(posts_data)
    
    if not transformed_data:
        logger.error("No data to transform")
        return False
    
    # Load data
    logger.info("Loading data to MongoDB")
    with ETLLoader(MONGO_URI, DB_NAME) as loader:
        success = loader.load_data(COLLECTION_NAME, transformed_data)
        
    if success:
        logger.info("ETL pipeline completed successfully")
    else:
        logger.error("ETL pipeline failed")
        
    return success

if __name__ == "__main__":
    run_etl_pipeline()