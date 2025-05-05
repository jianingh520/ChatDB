import nosql_functions as nosql_func
from nlp_processor import NLPProcessor

class DatabaseLayer:
    def __init__(self, client):
        self.client = client
        
    def list_databases(self):
        return self.client.list_database_names()
    
    def start(self):
        while True:
            db_list = "\n".join(self.list_databases())
            print(
            "\n--- NoSQL is selected ---\n"
            f"Available databases:\n{db_list}\n"
            "\nTo proceed, choose an existing database or create a new one with: 'create database [database name]'.\n"
            "Use 'exit' to return to the main menu.\n")
            
            db_name = input("Enter database name or create database (or 'exit' to main menu): ").strip()
            db_name = db_name.lower()
            
            if db_name == 'exit':
                return None
            
            if db_name == 'mainmenu':
                return None
            
            if db_name in self.list_databases():
                return db_name
            
            if db_name.startswith('create database '):
                db_name = db_name.split()[2]
                database = nosql_func.set_database(self.client, db_name)
                print(f"database: {db_name} created!\n")
                return db_name
            
            print("Invalid database. Please try again.")
    