import os
import sys

# Sql
from set_up_db import check_database_exists, connect_mysql_no_db, create_database, upload_dataset_to_mysql
from sample_queries import connect_to_mysql, generate_sql_examples, execute_query 
from nlp_matching import map_natural_language_to_query 

# NoSql
from pymongo import MongoClient
from database_layer import DatabaseLayer
from collection_layer import CollectionLayer
from operation_layer import OperationLayer
import nosql_functions as nosql_func
from example_generator import ExampleGenerator



def handle_main_menu():
    while True:
        print("--- MAIN MENU ---")
        print("--- DATABASE TYPE SELECTION ---")
        print("1: SQL (MySQL)")
        print("2: NoSQL (MongoDB)")
        print("3: Exit")
        
        # Choose database type
        user_input = input("Select the database type (1/2/3): ").strip()
        
        if user_input == '1':
            return 'SQL'
        
        elif user_input == '2':
            return 'NOSQL'
               
        elif user_input == '3':
            return "EXIT"
        
        else:
            print("Invalid choice. Please try again.\n")
        

def main():
    print("Welcome to ChatDB!")

    # init NoSQL setting
    client_url = 'mongodb://localhost:27017/'  # mongoDB url
    client = MongoClient(client_url)
    database = None
    collection = None
    
    #Initial state
    # state for flow control
    current_state = 'MAIN_MENU'
    
    while current_state != 'EXIT':
        if current_state == 'MAIN_MENU':
            current_state = handle_main_menu()
        
        # Handle SQL
        elif current_state == 'SQL': 
            # Connect to MySQL without selecting a database, return show databases result
            
            connect_no_db = connect_mysql_no_db()
            print("\n--- SQL is selected ---")
            print(f"Available databases:")
            db_list = execute_query(connect_no_db,f"SHOW DATABASES", as_list=True)
            for db in db_list:
                print(f"{db}")
            print("\nTo proceed, choose an existing database or create a new one with: 'create database [database name]'.\n"
                  "Use 'exit' to return to the main menu.\n")

            db_name = input("Enter database name or create database (or 'exit' to main menu): ").strip() # get db name

            if db_name == 'exit':
                current_state = 'MAIN_MENU'  # Back to database type selection

            elif db_name.startswith('create database '):   
                db_name = db_name.split()[2]
                # Create Database
                if check_database_exists(connect_no_db, db_name):
                    print(f"Database '{db_name}' already exists.")
                else:
                    # Create the database if it does not exist
                    create_database(connect_no_db, db_name)
                    print(f"Database '{db_name}' created!\n")
                    continue
            elif db_name in db_list:
                connection = connect_to_mysql(db_name)
                if connection:
                    print(f"Database '{db_name}' connected!")
                    current_state = 'TABLE'
            else:
                print("Invalid database. Please try again.")
                continue


        # SQL Table layer
        elif current_state == 'TABLE':

            # Show Table Name: show tables
            print(f"\n--- Database: {db_name} is selected ---")
            print(f"Available tables:")
            tb_list = execute_query(connection,f"SHOW TABLES", as_list=True)
            for tb in tb_list:
                print(f"{tb}")
            print("\nTo proceed, choose an existing table or create a new one with: 'create table [table name]'.\n"
                  "Use 'exit' to return to the main menu.\n")

            table_name = input("Enter table name: ").strip()

            if table_name == 'exit':
                current_state = 'SQL'  # Return to database layer

            elif table_name.startswith("create table "):
                table_name = table_name.split()[2]
                csv_path = input("Enter path to CSV file: ").strip()
                # Create Table
                if connection:
                    upload_dataset_to_mysql(connection, table_name, csv_path)
                    print(f"Table '{table_name}' created!\n")
                    continue
            elif table_name in tb_list:
            # Explore data set
                print("Attributes of All Columns:")
                print(execute_query(connection,f"DESCRIBE {table_name}"))
                print(f"\n Top 10 records of {table_name}:")
                print(execute_query(connection,f"SELECT * FROM {table_name} LIMIT 10"))
                current_state = 'SQL_OPERATION'
            else:
                print("Invalid table. Please try again.")
                #continue

        # SQL Operation_layer
        elif current_state == 'SQL_OPERATION':
            """
            Input:
                user_input: prevent ERROR TODO
            Return:
                1. example of sql query
                2. example of one of language constructs. e.g. group by, where, order by, aggregation
                3. nl matching
            """
            queries = generate_sql_examples(connection, table_name) # 
            user_input = input("Enter the type of example you want (e.g., 'example of GROUP BY', 'example of SQL query'): ").strip().lower()
            if user_input == 'exit':
                current_state = 'TABLE' # or GO BACK TO the begaining 

            elif user_input == "example of sql query":
                # Extract all query examples
                for query_type, examples in queries.items():
                    print(f"{query_type} Queries:")
                    for desc, query, result in examples:
                        print(f"Description: {desc}\nQuery: {query}\nResult:\n{result}\n")

            elif user_input.startswith("example of"):
                # Extract specific query type (e.g., GROUP BY, WHERE)
                query_type = user_input.replace("example of", "").strip().upper()
                if query_type in queries:
                    print(f"{query_type} Queries:")
                    for desc, query, result in queries[query_type]:
                        print(f"Description: {desc}\nQuery: {query}\nResult:\n{result}\n")
                else:
                    print(f"No examples available for {query_type}.")
            else:
                # Pass the input to NL matching
                cursor = connection.cursor()
                cursor.execute(f"DESCRIBE {table_name}")
                column_info = {row[0]: row[1] for row in cursor.fetchall()}               
                sql_query = map_natural_language_to_query(user_input, table_name, column_info)
                if sql_query:
                    print(f"Generated Query:\n{sql_query}\n")
                    # execute and show results
                    result = execute_query(connection, sql_query)
                    print(result)
                else:
                    print("Sorry, I couldn't understand your question. Please try again.")
                    
        # Handle NoSQL
        elif current_state == 'NOSQL':
            db_layer = DatabaseLayer(client)
            db_name = db_layer.start()
            
            if db_name is None:
                current_state = 'MAIN_MENU'  # Back to database type selection
            else:
                database = client[db_name]
                current_state = 'COLLECTION'
        
        # NoSQL Collection Layer    
        elif current_state == 'COLLECTION':
            col_layer = CollectionLayer(database)
            col_name = col_layer.start()
            
            if col_name == None:
                current_state = 'NOSQL'  # Return to database layer
            
            elif col_name == 'MAIN_MENU':
                current_state = col_name
            
            else:
                collection = database[col_name]
                current_state = 'OPERATION'
        
        # NOSQL Operation Layer        
        elif current_state == 'OPERATION':
            op_layer = OperationLayer(collection)
            operation_result = op_layer.start()
            
            if operation_result == 'EXIT':
                current_state = 'COLLECTION'  #可以再討論要回去哪一層
            
            elif operation_result == 'MAIN_MENU':
                current_state = operation_result
    
    # current_state == 'EXIT'
    print("Goodbye!")


# Main execution
if __name__ == "__main__":
    main()
