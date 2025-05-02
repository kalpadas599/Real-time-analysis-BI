# MongoDB client for storing raw data

import logging
from pymongo import MongoClient

logger = logging.getLogger(__name__)

class MongoDBClient:
    """Client for interacting with MongoDB"""
    
    def __init__(self, connection_string, database_name):
        """
        Initialize MongoDB client
        
        Args:
            connection_string (str): MongoDB connection string
            database_name (str): Name of the database
        """
        self.connection_string = connection_string
        self.database_name = database_name
        
        # Connect to MongoDB
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        
        logger.info(f"Connected to MongoDB database: {database_name}")
    
    def insert_one(self, collection_name, document):
        """
        Insert a single document into a collection
        
        Args:
            collection_name (str): Name of the collection
            document (dict): Document to insert
            
        Returns:
            str: ID of the inserted document
        """
        try:
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"Error inserting document into {collection_name}: {e}")
            return None
    
    def insert_many(self, collection_name, documents):
        """
        Insert multiple documents into a collection
        
        Args:
            collection_name (str): Name of the collection
            documents (list): List of documents to insert
            
        Returns:
            list: List of IDs of the inserted documents
        """
        try:
            collection = self.db[collection_name]
            result = collection.insert_many(documents)
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            logger.error(f"Error inserting documents into {collection_name}: {e}")
            return []
    
    def find_one(self, collection_name, query):
        """
        Find a single document in a collection
        
        Args:
            collection_name (str): Name of the collection
            query (dict): Query to find the document
            
        Returns:
            dict: Found document
        """
        try:
            collection = self.db[collection_name]
            return collection.find_one(query)
        except Exception as e:
            logger.error(f"Error finding document in {collection_name}: {e}")
            return None
    
    def find_many(self, collection_name, query, limit=100):
        """
        Find multiple documents in a collection
        
        Args:
            collection_name (str): Name of the collection
            query (dict): Query to find the documents
            limit (int): Maximum number of documents to return
            
        Returns:
            list: List of found documents
        """
        try:
            collection = self.db[collection_name]
            return list(collection.find(query).limit(limit))
        except Exception as e:
            logger.error(f"Error finding documents in {collection_name}: {e}")
            return []
    
    def update_one(self, collection_name, query, update):
        """
        Update a single document in a collection
        
        Args:
            collection_name (str): Name of the collection
            query (dict): Query to find the document
            update (dict): Update to apply
            
        Returns:
            int: Number of documents modified
        """
        try:
            collection = self.db[collection_name]
            result = collection.update_one(query, update)
            return result.modified_count
        except Exception as e:
            logger.error(f"Error updating document in {collection_name}: {e}")
            return 0
    
    def delete_one(self, collection_name, query):
        """
        Delete a single document from a collection
        
        Args:
            collection_name (str): Name of the collection
            query (dict): Query to find the document
            
        Returns:
            int: Number of documents deleted
        """
        try:
            collection = self.db[collection_name]
            result = collection.delete_one(query)
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error deleting document from {collection_name}: {e}")
            return 0
    
    def close(self):
        """Close the MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("Closed MongoDB connection")