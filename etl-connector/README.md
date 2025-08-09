**Name:** Dharunika S
**Roll Number:** 3122225001026

# ETL Data Connector for JSONPlaceholder API

This is a Python ETL (Extract, Transform, Load) pipeline that connects to the JSONPlaceholder API, extracts post data, transforms it, and loads it into a MongoDB collection.

## API Information

- **Base URL**: https://jsonplaceholder.typicode.com
- **Endpoint Used**: `/posts`
- **Authentication**: None (public API)
- **Rate Limits**: None (for this demo API)

## Setup Instructions

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd etl-connector
   ```
2. Install dependencies: `pip install -r requirements.txt`
3. Run ETL: `python etl_connector.py`
4. Check MongoDB:
   `mongo`
   `use etl_data`
   `db.posts_raw.find().pretty()`

## Data Model

```json
{
  "post_id": 1,
  "user_id": 1,
  "title": "sunt aut facere repellat provident occaecati excepturi optio reprehenderit",
  "body": "quia et suscipit...",
  "ingestion_timestamp": "2023-11-15T12:34:56.789Z"
}
```
