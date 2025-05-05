import nosql_functions as nosql_func
from nlp_processor import NLPProcessor
from example_generator import ExampleGenerator
from keywords import match_stage, group_stage, sort_stage, build_pipeline

class OperationLayer:
    def __init__(self, collection):
        self.collection = collection
        self.nlp = NLPProcessor()
        self.example_generator = ExampleGenerator(collection.name)
    
    # helper funcitons
    def execute_find(self, query, project={"_id":0}, limit=None):
        try:
            if limit is None:
                results = list(self.collection.find(query, project))
            else:
                results = list(self.collection.find(query, project).limit(limit))
    
            if results:
                for result in results:
                    print(result)
                    print()
            else:
                print("No documents found.")
        except Exception as e:
            print(f"Error executing find query: {e}")    
    
    
    def execute_aggregate(self, pipeline=None, match=None, group=None, sort=None, project={"_id":0}):
        if pipeline:
            pipeline = pipeline
        else:
            pipeline = build_pipeline(match=match, group=group, sort=sort, project=project)
            
        try:
            results = list(self.collection.aggregate(pipeline))
            if results:
                for result in results:
                    print(result)
                    print()
            else:
                print("No results from pipeline")
        except Exception as e:
            print(f"Error executing pipeline: {e}")
            
    def execute_sort(self, field, order, limit=None):
        try:
            order_flag = 1 if order == "ascending" else -1
            if limit is None:
                results = list(self.collection.find().sort(field, order_flag))
            else:
                results = list(self.collection.find().sort(field, order_flag).limit(limit))
            
            if results:
                for result in results:
                    print(result)
                    print()
            else:
                print("No documents found.")
        except Exception as e:
            print(f"Error executing sort query: {e}")
    
    def execute_query(self, query_str):
        if ".find(" in query_str:
            query = eval(query_str.split(".find(", 1)[1].rstrip(")"))
            return list(self.collection.find(query).limit(3))  # limit results to 3
        elif ".aggregate(" in query_str:
            pipeline = eval(query_str.split(".aggregate(", 1)[1].rstrip(")"))
            return list(self.collection.aggregate(pipeline))[:3]  # limit results to 3
        elif ".distinct(" in query_str:
            query = eval(query_str.split(".distinct(", 1)[1].rstrip(")"))
            return list(self.collection.distinct(query))
        else:
            print("Unsupported query format. ")
            return []
        
    def show_example_and_execute(self):
        print("\n--- NoSQL Query Examples ---")
        examples = self.example_generator.generate_collection_examples(self.collection)
        
        for example in examples:
            description = example["description"]
            query_str = example["query"]
            
            print(f"{description}: ")
            print("Query: " + query_str)
            
            results = self.execute_query(query_str)
            if results:
                print("Results:")
                
                for result in results:
                    print(result)
                    if ".distinct(" not in query_str:
                        print()
            else:
                print("No results found.")
            
            print()
                
    
    def show_examples_with_keyword_and_execute(self, keyword):
        print(f"\n--- Example Queries Containing '{keyword}' ---")
        examples = self.example_generator.generate_collection_examples(self.collection)
        filtered_examples = self.example_generator.filter_examples_by_keyword(examples, keyword)
        
        if not filtered_examples:
            print(f"No examples found with the keyword '{keyword}'.")
            return
        
        for example in filtered_examples:
            description = example["description"]
            query_str = example["query"]

            print(f"{description}: ")
            print("Query: " + query_str)

            results = self.execute_query(query_str)
            if results:
                print("Results:")
                for result in results:
                    print(result)
                    if ".distinct(" not in query_str:
                        print()
            else:
                print("No results found.")
                
            print()

    
    # main function
    def start(self):
        """Handle user operations"""
        print(f"\n--- Collection: {self.collection.name} is selected ---")
        print("--- First 3 Documents in the Collection ---")
        self.execute_find(query={}, limit=3)  # print first 3 data of the collection
        
        while True:
            print("-------------------------------------------")
            user_input = input("Enter your message to chatDB (or 'exit' to return to collection page): ").strip()
            #user_input = user_input.lower()
            
            if user_input == 'exit':
                return 'EXIT'
            
            if user_input == 'maninmenu':
                return 'MAIN_MENU'
            
                
            # Parse user input
            parsed_query = self.nlp.parse_input(user_input)
            intent = parsed_query["intent"]
            params = parsed_query["params"]
            
            # Generate Examples
            if intent == 'example_queries':
                self.show_example_and_execute()
            
            elif intent == 'example_with_keyword':
                keyword = params.get("keywords")
                if not keyword:
                    print("No keyword provided for filtering.")
                    continue
                self.show_examples_with_keyword_and_execute(keyword)
                
                
            # Do nlp
            # Find
            elif intent == 'find_all':
                try:
                    print(f"Executing: db.{self.collection.name}.find()")
                    query = {}
                    projection = {"_id":0}
                    
                    print("Query Results:")
                    self.execute_find(query,projection)
                except Exception as e:
                    print(f"Error executing find_all: {e}")
                    
            elif intent == "find_equals":
                try:
                    field = params['field']
                    value = params['value']
                    # determine type (numeric or string)
                    value = float(value) if value.replace('.', '', 1).isdigit() else value
                    query = {field: value}
                    print(f"Executing: db.{self.collection.name}.find({query})")
                    print("Query Results:")
                    self.execute_find(query=query)
                except Exception as e:
                    print(f"Error executing find_equals: {e}")
       
            elif intent == 'find_all_where':
                try:
                    field = params['field']
                    operator = params['operator']
                    value = params['value']
                    value = float(value) if value.replace('.', '', 1).isdigit() else value
                    query = {field: {f"${operator}": value}}
                    print(f"Executing: db.{self.collection.name}.find({query})")
                    self.execute_find(query)
                except Exception as e:
                    print(f"Error executing find_all_where: {e}")
                            
            elif intent == 'find_where':
                try:
                    str_field = params["str_field"]
                    num_field = params["num_field"]
                    operator = params["operator"]
                    value = params["value"]
                    # Determine type (numeric or string)
                    value = float(value) if value.replace('.', '', 1).isdigit() else value
                    
                    query = {num_field: {f"${operator}": value}}
                    projection = {str_field:1, "_id":0}
                    
                    print(f"Executing: db.{self.collection.name}.find({query}, {projection})")
                    print("Query Results:")
                    self.execute_find(query, projection)
                except Exception as e:
                    print(f"Error executing find: {e}")
                    
                    
            elif intent == "find_column":
                try:
                    field = params["field"]
                    print(f"Executing: db.{self.collection.name}.find({{{field}: 1, '_id': 0}})")
                    results = list(self.collection.find({}, {field: 1, "_id": 0}))  # Retrieve only the specified column
                    if results:
                        print(f"Values for column '{field}':")
                        for result in results:
                            print(result)
                    else:
                        print(f"No values found for column '{field}'.")
                        
                except Exception as e:
                    print(f"Error executing find_column: {e}")
                    
            # aggregation part
            elif intent in ['aggregate_sum', 'aggregate_avg', 'aggregate_count']:
                try:
                    field = params['field'] if intent != 'aggregate_count' else None
                    group = params['group']
                    pipeline = [
                        {
                            "$group": {
                                "_id": f"${group}",
                                f"{'total_' + field if intent == 'aggregate_sum' else 'average_' + field if intent == 'aggregate_avg' else 'count'}":
                                    {"$sum": f"${field}" if intent == 'aggregate_sum' else 1 if intent == 'aggregate_count' else f"${field}"}
                            }
                            
                        },
                        {
                            "$sort": { "_id": 1}
                        }
                    ]
                    print(f"Executing: db.{self.collection.name}.aggregate({pipeline})")
                    print("Query Results:")
                    self.execute_aggregate(pipeline=pipeline)
                    
                except Exception as e:
                    print(f"Error executing aggregation ({intent}): {e}")
            
            # group by part
            elif intent == 'group_by':
                try:
                    pipeline = [
                        {"$group": {"_id": f"${params['group']}", "count": {"$sum": 1}}}
                    ]
                    
                    print(f"Executing: db.{self.collection.name}.aggregate({pipeline})")
                    print("Query Results:")
                    self.execute_aggregate(pipeline=pipeline)
                    
                except Exception as e:
                    print("Error executing aggregation:", e)
                    
            # sort part
            elif intent == 'sort':
                try:
                    field = params['field']
                    order = 1 if params['order'] == 'ascending' else -1
                    print(f"Executing: db.{self.collection.name}.find().sort({{{field}: {order}}})")
                    print("Query Results:")
                    self.execute_sort(field, order)
                                        
                except Exception as e:
                    print("Error executing aggregation:", e)
                    
                    
            # having
            elif intent == "having":
                try:
                    agg_type = 'sum' if params['aggregate_type'] == 'total' else 'avg'
                    agg_name = params['aggregate_type'] + "_" + params["field"]
                    pipeline = [
                        {"$group": {"_id": f"${params['group']}", agg_name: {f"${agg_type}": f"${params['field']}"}}},
                        {"$match": {agg_name: {f"${params['operator']}": int(params["value"])}}}
                    ]
                    print(f"Executing: db.{self.collection.name}.aggregate({pipeline})")
                    print("Query Results:")
                    self.execute_aggregate(pipeline)
                except Exception as e:
                    print(f"Error executing having: {e}")
                    
            # join
            elif intent == "join":
                try:
                    pipeline = [
                        {
                            "$lookup": {
                                "from": params["to_collection"],
                                "localField": params["local_field"],
                                "foreignField": "_id",
                                "as": params["as_field"]
                            }
                        },
                        {
                            "$project": {"_id": 0}
                        }
                    ]
                    print(f"Executing: db.{self.collection.name}.aggregate({pipeline})")
                    print("Query Results:")
                    self.execute_aggregate(pipeline)
                except Exception as e:
                    print(f"Error executing join: {e}")
                    
            else:
                print("Sorry, I don't understand. Please try again.")