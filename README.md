# Vertigo Games Data Engineer Use Case 
### Burak Albayrak 


#### Tools I have used
- Python, Flask  
- Google Cloud Run  
- Google Cloud Bucket (for storing Docker images)  
- PostgreSQL in Google Cloud SQL  
- DBeaver for database connection tests  
- Docker  
- gcloud CLI (for pushing images to Artifact Registry)  


#### My Approach


- First, run `insert_data.py` for the initial data load to make data available in the database.
- The database is live in Google Cloud.
- The REST API is handled with the Python Flask framework and runs on google cloud run system.
- API calls are saved as a Postman collection file, which can easily be tested.

## Public API Link
- https://vertigoimg-108401742225.europe-west1.run.app
