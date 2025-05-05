import json
from datetime import datetime

from bson import ObjectId
import pandas as pd
import pymongo
import requests


client_url = 'mongodb://localhost:27017/'
client = pymongo.MongoClient(client_url)

# helper function
def set_client(url):
    client_url = url
    client = pymongo.MongoClient(client_url)
    return client

def transform_document(doc):
    if isinstance(doc, dict):
        for key, value in doc.items():
            if isinstance(value, dict):
                if "$oid" in value:
                    doc[key] = ObjectId(value["$oid"])
                elif "$date" in value:
                    doc[key] = datetime.fromisoformat(value["$date"].replace('Z', '+00:00'))
                else:
                    transform_document(value)
            elif isinstance(value, list):
                for item in value:
                    transform_document(item)
    return doc


# DATABASE CRUD
# CREATE (automatically in mongodb when access a new database)
def set_database(client, db_name):
    database = client[db_name]
    return database

# CREATE (automatically in mongodb when the first document is inserted)
def set_collection(database, collection_name):
    collection = database[collection_name]
    return collection

def create_collection(database, collection_name):
    database.create_collection(collection_name)
    return


# POST
# can be seen as upload the whole dataset (Will create both the collection and the database)
def insert_many(collection, data):
    collection.insert_many(data)
    return

# Load JSON data
# not sure data would contain all collection in one data or ?
def upload_JSON_data(collection, json_file):
    try:
        with open(json_file) as f:
            file_data = json.load(f)
            
        if isinstance(file_data, list):
            file_data = [transform_document(doc) for doc in file_data]
            collection.insert_many(file_data)
        else:
            file_data = transform_document(file_data)
            collection.insert_one(file_data)
            
        return collection.find()
    except Exception as e:
        print(f"Error: {e}")

# READ/GET
def find(collection, query):
    query = json.loads(query)
    return collection.find(query)

def aggregate(collection, pipeline):
    pipeline = json.loads(pipeline)
    return collection.aggregate(pipeline)
