import pymysql
import pandas as pd
from datetime import datetime
import sys

# connect to MySQL without specifying a database
def connect_mysql_no_db():
    try:
        connection = pymysql.connect(
            host='localhost', 
            user='root',  # MySQL username
            password='Dsci-551'  # MySQL password
        )
        print("Connected to MySQL (no specific database selected)")
        return connection
    except pymysql.MySQLError as err:
        print(f"Error: {err}")
        return None

# function to check if a database exists
def check_database_exists(connection, db_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
        result = cursor.fetchone()
        return result is not None
    except pymysql.MySQLError as err:
        print(f"Error checking database: {err}")
        return False

# function to create a new database in MySQL
def create_database(connection, db_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' created.")
        connection.commit()
    except pymysql.MySQLError as err:
        print(f"Error creating database: {err}")

# function to connect to a specific database in MySQL
def connect_mysql_with_db(db_name):
    try:
        connection = pymysql.connect(
            host='localhost', 
            user='root',  # MySQL username
            password='Dsci-551',  # MySQL password
            database=db_name  # Now connect to the specific database
        )
        print(f"Connected to MySQL database: {db_name}")
        return connection
    except pymysql.MySQLError as err:
        print(f"Error: {err}")
        return None

# function to detect date, time, or datetime using the datetime module
def detect_datetime_format(value):
    formats = [
        ("%Y-%m-%d", "DATE"),  # Date format
        ("%H:%M:%S", "TIME"),  # Time format
        ("%Y-%m-%d %H:%M:%S", "DATETIME"),  # Datetime format
        ("%m/%d/%Y", "DATE")  # Alternate date format (MM/DD/YYYY)
    ]
    
    for fmt, dtype in formats:
        try:
            datetime.strptime(value, fmt)
            return dtype, fmt  # Return the detected format along with the data type
        except (ValueError, TypeError):
            continue
    return None, None  # Return None if the format does not match any datetime pattern

# function to preprocess and format date columns before upload data to MySQL
def preprocess_dates(dataframe):
    for column in dataframe.columns:
        # try detect if the column is a date
        sample_value = dataframe[column].dropna().iloc[0] if not dataframe[column].dropna().empty else None
        if sample_value:
            dtype, detected_format = detect_datetime_format(str(sample_value))
            if dtype == "DATE" and detected_format:
                # convert the column to the SQL date format YYYY-MM-DD
                dataframe[column] = pd.to_datetime(dataframe[column], format=detected_format, errors='coerce').dt.strftime('%Y-%m-%d')

# function to map pandas data types to MySQL data types using datetime module
def map_pandas_dtype_to_mysql(column, dataframe):
    sample_value = dataframe[column].dropna().iloc[0] if not dataframe[column].dropna().empty else None

    if sample_value:
        # try detect datetime format using the datetime module
        detected_type, _ = detect_datetime_format(str(sample_value))
        if detected_type:
            return detected_type

    # default MySQL types if not date/time related
    dtype = dataframe[column].dtype
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DECIMAL(10, 2)"
    else:
        return "VARCHAR(255)"

# primary key detection using Uniqueness, Non-nullability, and Cardinality
def detect_primary_key(dataframe):
    total_rows = len(dataframe)
    
    for column in dataframe.columns:
        unique_values = dataframe[column].nunique()  # Number of unique values
        non_null_count = dataframe[column].notnull().sum()  # Non-null count
        cardinality = unique_values / total_rows  # Cardinality ratio

        # check if the column is a good fit for primary key
        if dataframe[column].is_unique and non_null_count == total_rows and cardinality > 0.9:
            print(f"Column '{column}' detected as the primary key (Unique, Non-null, High Cardinality).")
            return column

    print("No suitable primary key detected based on uniqueness, non-nullability, and cardinality.")
    return None  # if there is no suitable column is found

# drop the table if it already exists
def drop_existing_table(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        print(f"Table '{table_name}' dropped if it existed.")
    except pymysql.MySQLError as err:
        print(f"Error dropping table: {err}")

# create a table based on CSV column types
def create_table_from_csv(connection, table_name, dataframe):
    cursor = connection.cursor()

    # drop the table if it already exists
    drop_existing_table(connection, table_name)
    
    # find the primary key column by checking uniqueness, non-nullability, and cardinality
    primary_key_column = detect_primary_key(dataframe)
    if not primary_key_column:
        print("Error: No suitable primary key column found.")
        return
    
    # create column definitions based on CSV data types
    columns = []
    for column_name in dataframe.columns:
        col_type = map_pandas_dtype_to_mysql(column_name, dataframe)
        
        # detected unique column as the primary key
        if column_name == primary_key_column:
            columns.append(f"`{column_name}` {col_type} PRIMARY KEY")
        else:
            columns.append(f"`{column_name}` {col_type}")
    
    # query for CREATE TABLE
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {', '.join(columns)}
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    print(f"Table '{table_name}' created with columns based on CSV structure, using '{primary_key_column}' as the primary key.")

# upload dataset to MySQL based on CSV columns and table columns
def upload_dataset_to_mysql(connection, table_name, csv_file_path):
    try:

        dataset = pd.read_csv(csv_file_path)

        # date columns to correct the format
        preprocess_dates(dataset)
        
        # create the table based on the CSV structure
        create_table_from_csv(connection, table_name, dataset)
        
        # insert data from CSV into the table
        cursor = connection.cursor()
        for _, row in dataset.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join([f"`{col}`" for col in row.index])
            sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))
        connection.commit()
        print(f"Dataset uploaded to '{table_name}' based on matching CSV columns.")
    
    except FileNotFoundError as fnf_error:
        print(f"Error: {fnf_error}")
    except pymysql.MySQLError as err:
        print(f"Error during data upload: {err}")

# main execution
if __name__ == "__main__":

    if len(sys.argv) == 4:
        # If parameters are passed: User upload their own dataset
        DB_NAME = sys.argv[1]
        TABLE_NAME = sys.argv[2]
        CSV_PATH = sys.argv[3]
        print(f"User upload mode: Database: {DB_NAME}, Table: {TABLE_NAME}, CSV: {CSV_PATH}")
    else:
        DB_NAME = 'coffee_shop'
        TABLE_NAME = 'coffee_sales'
        CSV_PATH = 'coffee_shop_sales.csv'

    # connect to MySQL first without selecting a database
    mysql_conn = connect_mysql_no_db()

    if mysql_conn:
        # Step 1: Define the database name
        database_name = DB_NAME  # Replace with your desired database name
        
        # Step 2: Check if the database exists
        if check_database_exists(mysql_conn, database_name):
            print(f"Database '{database_name}' already exists.")
        else:
            # Create the database if it does not exist
            create_database(mysql_conn, database_name)

        # Step 3: Reconnect to the existing or newly created database
        mysql_conn_with_db = connect_mysql_with_db(database_name)
        
        if mysql_conn_with_db:
            # Dataset Setup (one time)
            coffee_shop_table_name = TABLE_NAME
            coffee_shop_csv_file_path = CSV_PATH
            
            # create the table and upload the dataset
            upload_dataset_to_mysql(mysql_conn_with_db, coffee_shop_table_name, coffee_shop_csv_file_path)

            # close the connection
            mysql_conn_with_db.close()
