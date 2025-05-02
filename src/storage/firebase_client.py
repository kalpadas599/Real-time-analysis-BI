# Firebase client for real-time updates

import logging
import json
import firebase_admin
from firebase_admin import credentials, db

logger = logging.getLogger(__name__)

class FirebaseClient:
    """Client for interacting with Firebase Realtime Database"""
    
    def __init__(self, credentials_path, database_url):
        """
        Initialize Firebase client
        
        Args:
            credentials_path (str): Path to Firebase credentials JSON file
            database_url (str): Firebase database URL
        """
        self.credentials_path = credentials_path
        self.database_url = database_url
        
        # Initialize Firebase app
        try:
            cred = credentials.Certificate(credentials_path)
            self.app = firebase_admin.initialize_app(cred, {
                'databaseURL': database_url
            })
            logger.info("Connected to Firebase Realtime Database")
        except Exception as e:
            logger.error(f"Error connecting to Firebase: {e}")
            self.app = None
    
    def update(self, path, data):
        """
        Update data at a specific path
        
        Args:
            path (str): Path in the database
            data (dict): Data to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert any non-serializable objects to strings
            data_json = json.dumps(data)
            data = json.loads(data_json)
            
            # Update data
            ref = db.reference(path)
            ref.update(data)
            
            logger.info(f"Updated data at path: {path}")
            return True
        except Exception as e:
            logger.error(f"Error updating data at path {path}: {e}")
            return False
    
    def set(self, path, data):
        """
        Set data at a specific path
        
        Args:
            path (str): Path in the database
            data (dict): Data to set
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert any non-serializable objects to strings
            data_json = json.dumps(data)
            data = json.loads(data_json)
            
            # Set data
            ref = db.reference(path)
            ref.set(data)
            
            logger.info(f"Set data at path: {path}")
            return True
        except Exception as e:
            logger.error(f"Error setting data at path {path}: {e}")
            return False
    
    def get(self, path):
        """
        Get data from a specific path
        
        Args:
            path (str): Path in the database
            
        Returns:
            dict: Data at the path
        """
        try:
            ref = db.reference(path)
            data = ref.get()
            
            logger.info(f"Got data from path: {path}")
            return data
        except Exception as e:
            logger.error(f"Error getting data from path {path}: {e}")
            return None
    
    def push(self, path, data):
        """
        Push data to a list at a specific path
        
        Args:
            path (str): Path in the database
            data (dict): Data to push
            
        Returns:
            str: Key of the pushed data
        """
        try:
            # Convert any non-serializable objects to strings
            data_json = json.dumps(data)
            data = json.loads(data_json)
            
            # Push data
            ref = db.reference(path)
            result = ref.push(data)
            
            logger.info(f"Pushed data to path: {path}")
            return result.key
        except Exception as e:
            logger.error(f"Error pushing data to path {path}: {e}")
            return None
    
    def delete(self, path):
        """
        Delete data at a specific path
        
        Args:
            path (str): Path in the database
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            ref = db.reference(path)
            ref.delete()
            
            logger.info(f"Deleted data at path: {path}")
            return True
        except Exception as e:
            logger.error(f"Error deleting data at path {path}: {e}")
            return False
    
    def close(self):
        """Close the Firebase connection"""
        if self.app:
            firebase_admin.delete_app(self.app)
            logger.info("Closed Firebase connection")