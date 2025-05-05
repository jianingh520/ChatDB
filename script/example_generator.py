from utils import analyze_collection, get_column_info_and_examples
from keywords import match_stage, group_stage, lookup_stage, sort_stage, project_stage

class ExampleGenerator:
    def __init__(self, collection_name):
        self.collection_name = collection_name
    
    def generate_collection_examples(self, collection):
        """
        Generate query examples based on the column types in the collection.
        """
        column_types = analyze_collection(collection)
        column_info, examples, _ = get_column_info_and_examples(collection)
        
        examples_list = []
        
        # Generate examples for numeric fields
        for field in column_types['numeric'][:1]:
            sample_values = examples.get(field, [])
            if sample_values:
                # Determine reasonable thresholds
                min_val, max_val = min(sample_values), max(sample_values)
                mid_val = (min_val + max_val) // 2
                
                # Example: Find documents with value greater than mid-point
                # where/match example
                query = {field: {"$gt": mid_val}}
                examples_list.append({
                    "description": f"Find documents where {field} > {mid_val}",
                    "query": f"db.{self.collection_name}.find({query})"
                })

                # Example: Group by another field with sum
                # group by example
                group_by_field = column_types['string'][0] if column_types['string'] else None
                if group_by_field:
                    group_query = group_stage(agg_type='avg', group_by_field=group_by_field, aggregate_field=field)
                    examples_list.append({
                        "description": f"Average {field} grouped by {group_by_field}",
                        "query": f"db.{self.collection_name}.aggregate([{group_query}])"
                    })
                    
                    # Group by with HAVING example
                    having_query = [
                            group_stage(agg_type='avg', group_by_field=group_by_field, aggregate_field=field),
                            match_stage({f"average_{field}": {"$gt": mid_val}})
                    ]
                    examples_list.append({
                        "description": f"Average {field} grouped by {group_by_field} and filtered by average_{field} > {mid_val}",
                        "query": f"db.{self.collection_name}.aggregate({having_query})"
                    })
                
                # Sort examples for numeric fields    
                examples_list.append({
                    "description": f"Sort documents by {field} in ascending order",
                    "query": f"db.{self.collection_name}.aggregate([{sort_stage(field=field, order=1)}])"
                })

                    
                    
        # Generate examples for string fields
        for field in column_types['string'][:1]:
            sample_values = examples.get(field, [])
            if sample_values:
                # Use the first string value as the condition
                sample_value = sample_values[0]
                
                # Example: Find documents where the field equals a value
                query = {field: sample_value}
                examples_list.append({
                    "description": f"Find documents where {field} equals '{sample_value}'",
                    "query": f"db.{self.collection_name}.find({query})"
                })
                
                # Example: Group by this string field
                group_query = group_stage(agg_type='count', group_by_field=field, aggregate_field="count")
                examples_list.append({
                    "description": f"Group documents by {field}",
                    "query": f"db.{self.collection_name}.aggregate([{group_query}])"
                })
                
                # Sort examples for string fields
                examples_list.append({
                    "description": f"Sort documents by {field} in descending orders",
                    "query": f"db.{self.collection_name}.aggregate([{sort_stage(field=field, order=-1)}])"
                })
                
                # distinct
                examples_list.append({
                    "description": f"Retrieve all distinct values for the field '{field}'",
                    "query": f"db.{self.collection_name}.distinct('{field}')"
                })
                
        
        return examples_list
    
    
    def filter_examples_by_keyword(self, examples, keyword):
        return [
            ex for ex in examples
            if keyword.lower() in ex["description"].lower() or keyword.lower() in ex["query"].lower()
        ]