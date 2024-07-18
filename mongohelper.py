import os
import json
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables from .env file
load_dotenv()

# Get MongoDB connection URI from environment variables
mongo_uri = os.getenv('MONGO_URI')

# Connect to MongoDB
client = MongoClient(mongo_uri)

# Access your database and collection (replace 'your_database' and 'your_collection' with actual names)
db = client.your_database
collection = db.your_collection

# Example JSON data (you can load this from a file or any source)
json_data = {
    "name": "John Doe",
    "age": 30,
    "email": "johndoe@example.com"
}

# Insert the JSON data into MongoDB
collection.insert_one(json_data)

# Optionally, you can insert multiple documents from a list of JSON objects:
# documents = [json_data1, json_data2, ...]
# collection.insert_many(documents)

# Close the MongoDB connection
client.close()
