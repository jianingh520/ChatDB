from datetime import datetime

# Not used
def detect_column_type(values):
    if all(isinstance(v, (int, float)) for v in values if v is not None):
        return 'numeric'
    elif all(isinstance(v,str) for v in values if v is not None):
        return 'string'
    else:
        return 'other_type'


def get_column_info_and_examples(collection):
    """
    Retrieves field names, inferred data types, and example values from a MongoDB collection.
    """
    column_info = {}
    examples = {}
    all_columns = []

    # Get a sample of documents from the collection
    sample_docs = list(collection.find().limit(5))  # Limit to 5 documents for examples

    # If there are no documents, return empty results
    if not sample_docs:
        return column_info, examples, all_columns

    # Infer field types and collect sample values
    for doc in sample_docs:
        for field, value in doc.items():
            if field not in column_info:
                column_info[field] = {'type': type(value).__name__}  # Infer type
                examples[field] = [value]  # Start collecting examples
                all_columns.append(field)
            else:
                # Append examples up to the sample limit
                if len(examples[field]) < 5:
                    examples[field].append(value)

    return column_info, examples, all_columns



def analyze_collection(collection):
    """
    Categorize fields by type using `get_column_info_and_examples`.
    """
    column_info, _, _ = get_column_info_and_examples(collection)

    # Initialize categories
    column_types = {"numeric": [], "string": [], "other_type": []}

    # Categorize based on type
    for field, info in column_info.items():
        field_type = info["type"]
        
        if field == 'id':
            column_types['other_type'].append(field)
        elif field_type in ["int", "float"]:  # Numeric fields
            column_types["numeric"].append(field)
        elif field_type == "str":  # String fields
            column_types["string"].append(field)
        else:  # Other types
            column_types["other_type"].append(field)

    return column_types