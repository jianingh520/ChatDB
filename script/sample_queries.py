import sys
import pymysql
import pandas as pd

# connect to MySQL
def connect_to_mysql(database):
    try:
        connection = pymysql.connect(
        host='localhost',
        user='root',
        password='Dsci-551',
        database=database)
        print(f"Connected to MySQL, Database Name: {database}")
        return connection
    except pymysql.MySQLError as err:
        print(f"Error: {err}")
        return None


# get column types, keys, and example values
def get_column_info_and_examples(connection, table_name):
    """
    get column names, data types, keys, and example values from a MySQL table.
    """
    column_info = {}
    examples = {}
    all_columns = []
    
    with connection.cursor() as cursor:
        # Get column types and keys
        cursor.execute(f"DESCRIBE {table_name}")
        for row in cursor.fetchall():
            column_name, column_type, key_type = row[0], row[1], row[3]
            column_info[column_name] = {'type': column_type, 'key': key_type}
            all_columns.append(column_name)
        
        # Get example values
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        rows = cursor.fetchall()
        if rows:
            for i, column in enumerate(cursor.description):
                examples[column[0]] = [row[i] for row in rows]
    
    return column_info, examples, all_columns

# execute a query and return results
def execute_query(connection, query, as_list=False):
    """
    executes a query returns the result as a DF.
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        if as_list:
            return [row[0] for row in rows]
        return pd.DataFrame(rows, columns=columns)

# generate SQL examples based on query type
def generate_sql_examples(connection, table_name):
    """
    generates SQL query examples for all language constructs: GROUP BY, WHERE, HAVING, ORDER BY, AGGREGATION.
    """
    column_info, examples, all_columns = get_column_info_and_examples(connection, table_name)
    queries = {'GROUP BY': [], 'WHERE': [], 'HAVING': [], 'ORDER BY': [], 'AGGREGATION': []}

    # Separate columns into categorical and numeric, excluding primary and foreign keys from numeric operations
    categorical_columns = [
        col for col, info in column_info.items() 
        if ("id" not in col.lower()) and ("varchar" in info['type'] or "text" in info['type'] or "enum" in info['type']) and info['key'] not in ('PRI', 'MUL')
    ]
    numeric_columns = [
        col for col, info in column_info.items() 
        if ("id" not in col.lower()) and ("int" in info['type'] or "decimal" in info['type'] or "float" in info['type']) and info['key'] not in ('PRI', 'MUL')
    ]
    
    # GROUP BY queries
    for cat_col in categorical_columns[:2]:  # Limit to 2 examples
        description = f"Group {table_name} by {cat_col}"
        query = f"SELECT {cat_col}, COUNT(*) AS count FROM {table_name} GROUP BY {cat_col}"
        queries['GROUP BY'].append((description, query, execute_query(connection, query)))
    
    # WHERE queries
    # Numeric WHERE
    for num_col in numeric_columns[:1]:  # Limit to 1 example
        valid_value = max(examples[num_col]) if examples[num_col] else 100
        reordered_columns = [num_col] + [col for col in all_columns if col != num_col]
        description = f"Filter {table_name} where {num_col} is greater than {valid_value}"
        query = f"SELECT {', '.join(reordered_columns)} FROM {table_name} WHERE {num_col} > {valid_value}"
        queries['WHERE'].append((description, query, execute_query(connection, query)))
    
    # Categorical WHERE
    for cat_col in categorical_columns[:1]:  # Limit to 1 example
        valid_value = examples[cat_col][0] if examples[cat_col] else 'example_value'
        reordered_columns = [cat_col] + [col for col in all_columns if col != cat_col]
        description = f"Filter {table_name} where {cat_col} equals '{valid_value}'"
        query = f"SELECT {', '.join(reordered_columns)} FROM {table_name} WHERE {cat_col} = '{valid_value}'"
        queries['WHERE'].append((description, query, execute_query(connection, query)))
    
    # HAVING queries
    for cat_col in categorical_columns[:1]:  # Limit to 1 example
        for num_col in numeric_columns[:1]:  # Limit to 1 example
            group_value = sum(examples[num_col]) // 2 if examples[num_col] else 100
            description = f"Filter groups of {cat_col} where total {num_col} exceeds {group_value}"
            query = (
                f"SELECT {cat_col}, SUM({num_col}) AS total_{num_col} "
                f"FROM {table_name} GROUP BY {cat_col} HAVING total_{num_col} > {group_value}"
            )
            queries['HAVING'].append((description, query, execute_query(connection, query)))
    
    # ORDER BY queries
    for num_col in numeric_columns[:2]:  # Limit to 2 examples
        description = f"Sort {table_name} by {num_col} in descending order"
        reordered_columns = [num_col] + [col for col in all_columns if col != num_col]
        query = f"SELECT {', '.join(reordered_columns)} FROM {table_name} ORDER BY {num_col} DESC LIMIT 10"
        queries['ORDER BY'].append((description, query, execute_query(connection, query)))
    
    # AGGREGATION queries
    for cat_col in categorical_columns[:1]:  # Limit to 1 grouping column
        # SUM
        description = f"Total {numeric_columns[0]} by {cat_col}" if numeric_columns else "Total by group"
        query = (
            f"SELECT {cat_col}, SUM({numeric_columns[0]}) AS total_{numeric_columns[0]} "
            f"FROM {table_name} GROUP BY {cat_col}"
        ) if numeric_columns else ""
        if query:
            queries['AGGREGATION'].append((description, query, execute_query(connection, query)))
        
        # AVG
        description = f"Average {numeric_columns[0]} by {cat_col}" if numeric_columns else "Average by group"
        query = (
            f"SELECT {cat_col}, AVG({numeric_columns[0]}) AS avg_{numeric_columns[0]} "
            f"FROM {table_name} GROUP BY {cat_col}"
        ) if numeric_columns else ""
        if query:
            queries['AGGREGATION'].append((description, query, execute_query(connection, query)))
        
        # COUNT
        description = f"Count rows by {cat_col}"
        query = f"SELECT {cat_col}, COUNT(*) AS count_rows FROM {table_name} GROUP BY {cat_col}"
        queries['AGGREGATION'].append((description, query, execute_query(connection, query)))
    
    return queries

# main function to handle user requests with sys.argv
def main():
    if len(sys.argv) < 4:
        print("Usage: python script.py <database> <table> <query_type>")
        print("Use 'ALL' as <query_type> to fetch all examples.")
        sys.exit(1)

    database = sys.argv[1]
    table_name = sys.argv[2]
    query_type = sys.argv[3]

    connection = connect_to_mysql(database)
    queries = generate_sql_examples(connection, table_name)

    if query_type.upper() == "ALL":
        for query_type, examples in queries.items():
            print(f"{query_type} Queries:")
            for desc, query, result in examples:
                print(f"{desc}\nQuery:\n{query}\nResult:\n{result}\n")
    elif query_type in queries:
        print(f"{query_type} Queries:")
        for desc, query, result in queries[query_type]:
            print(f"{desc}\nQuery:\n{query}\nResult:\n{result}\n")
    else:
        print(f"Query type '{query_type}' is not supported.")

if __name__ == "__main__":
    main()
