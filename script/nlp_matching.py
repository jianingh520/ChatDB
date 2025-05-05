import re
import sys
import pymysql
import pandas as pd
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from sample_queries import connect_to_mysql, execute_query

#stemmer = PorterStemmer()

def preprocess_query(nl_query):
    tokens = word_tokenize(nl_query.lower())
    #stemmed_tokens = [stemmer.stem(token) for token in tokens]
    return " ".join(tokens)

def map_natural_language_to_query(nl_query, table_name, column_info):
    """
    Maps a natural language query to a specific SQL query pattern.

    Args:
        nl_query (str)
        table_name (str)
        column_info (dict)

    Returns:
        str: The generated SQL query.
    """
    nl_query = preprocess_query(nl_query)
    all_columns = list(column_info.keys())

    # Patterns for HAVING -> Modified
    if "having" in nl_query:
        match = re.search(r"having total (\w+) (greater|less) than (\d+) group by (\w+)", nl_query)
        if match:
            having_col, condition, value, group_col = match.groups()
            operator = ">" if condition == "greater" else "<"
            if having_col in column_info and group_col in column_info:
                return (
                    f"SELECT {group_col}, SUM({having_col}) AS total_{having_col} "
                    f"FROM {table_name} GROUP BY {group_col} HAVING total_{having_col} {operator} {value}"
                )


    # Patterns for GROUP BY and Aggregation
    if "group by" in nl_query or  "total" in nl_query or "average" in nl_query or "count" in nl_query:
        match = re.search(r"(total|average|count) (\w+)? (by|group by) (\w+)", nl_query)
        if match:
            agg_type, agg_col, _, group_col = match.groups()
            if group_col in column_info:
                if agg_type == "total" and agg_col in column_info:
                    return (
                        f"SELECT {group_col}, SUM({agg_col}) AS total_{agg_col} "
                        f"FROM {table_name} GROUP BY {group_col}"
                    )
                elif agg_type == "average" and agg_col in column_info:
                    return (
                        f"SELECT {group_col}, AVG({agg_col}) AS average_{agg_col} "
                        f"FROM {table_name} GROUP BY {group_col}"
                    )
                elif agg_type == "count":
                    return (
                        f"SELECT {group_col}, COUNT(*) AS count_rows "
                        f"FROM {table_name} GROUP BY {group_col}"
                    )
    
    # Patterns for WHERE (Numeric and Categorical)
    elif "filter" in nl_query or "where" in nl_query:
        # Numeric WHERE
        match_numeric = re.search(r"(filter|where) (\w+) (greater|less) than (\d+)", nl_query)
        if match_numeric:
            _, filter_col, condition, value = match_numeric.groups()
            operator = ">" if condition == "greater" else "<"
            if filter_col in column_info:
                reordered_columns = [filter_col] + [col for col in all_columns if col != filter_col]
                return f"SELECT {', '.join(reordered_columns)} FROM {table_name} WHERE {filter_col} {operator} {value}"

        # Categorical WHERE
        match_categorical = re.search(r"(filter|where) (\w+) equals (.+)", nl_query)
        if match_categorical:
            _, filter_col, value = match_categorical.groups()
            if filter_col in column_info:
                reordered_columns = [filter_col] + [col for col in all_columns if col != filter_col]
                value = value.strip("'")  # Ensure value is properly quoted
                return f"SELECT {', '.join(reordered_columns)} FROM {table_name} WHERE {filter_col} = '{value}'"
    
    # Patterns for ORDER BY
    elif "sort" in nl_query or "order by" in nl_query:
        match = re.search(r"(sort|order by) (\w+)", nl_query)
        if match:
            _, order_col = match.groups()
            if order_col in column_info:
                reordered_columns = [order_col] + [col for col in all_columns if col != order_col]
                return f"SELECT {', '.join(reordered_columns)} FROM {table_name} ORDER BY {order_col} DESC"
    

    return None

# Main function to handle natural language questions
def main():
    if len(sys.argv) < 3:
        print("Usage: python3 nlp_matching.py <database> <table>")
        sys.exit(1)

    database = sys.argv[1]
    table_name = sys.argv[2]

    connection = connect_to_mysql(database)

    # Get column information
    cursor = connection.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    column_info = {row[0]: row[1] for row in cursor.fetchall()}

    print("Enter your question (or type 'exit' to quit):")
    while True:
        user_input = input("> ").strip()
        if user_input.lower() == "exit":
            break

        # Map natural language to SQL query
        query = map_natural_language_to_query(user_input, table_name, column_info)
        if query:
            print(f"Generated Query:\n{query}\n")
            # Execute and show results
            result = execute_query(connection, query)
            print(result)
        else:
            print("Sorry, I couldn't understand your question. Please try again.")

if __name__ == "__main__":
    main()
