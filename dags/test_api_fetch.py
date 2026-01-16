import requests
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_fetch(start_time, end_time):
    base_url = "https://api.weather.gc.ca/collections/swob-realtime/items?f=json&limit=100"
    
    # Simulate the logic in the DAG
    # start_time and end_time are expected to be ISO strings like 2026-01-14T00:00:00Z
    time_filter = f"{start_time}/{end_time}"
    url = f"{base_url}&datetime={time_filter}"
    
    logger.info("Testing URL: %s", url)
    
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        
        features = data.get('features', [])
        logger.info("Features found: %d", len(features))
        
        if features:
            print("First feature sample:")
            print(features[0])
            
            # Check timestamps in features
            timestamps = [f['properties'].get('datetime') for f in features]
            print(f"Time range in data: {min(timestamps)} to {max(timestamps)}")
            
    except Exception as e:
        logger.error("Request failed: %s", e)
        if 'resp' in locals():
            logger.error("Response content: %s", resp.text)

if __name__ == "__main__":
    # Test for the date we are trying to backfill
    start = "2026-01-14T00:00:00Z"
    end = "2026-01-14T23:59:59Z"
    
    print(f"--- Testing range: {start} to {end} ---")
    test_fetch(start, end)
