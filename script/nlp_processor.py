import re

import keywords as keywords_query


class NLPProcessor:
    def __init__(self):
        self.patterns = {
            # Example Patterns
            "example_queries": r"example nosql queries",
            "example_with_keyword": r"example queries with (?P<keywords>.+)",
            
            # Find Patterns
            "find_all": r"find all data",
            "find_equals": r"find (?P<field>\w+)\s+(equals|is)\s+(?P<value>.+)",
            "find_all_where": r"find all where (?P<field>\w+) (?P<operator>>|>=|<|<=|=|!=) (?P<value>.+)",
            "find_where": r"find (?P<str_field>\w+) where (?P<num_field>\w+) (?P<operator>>|>=|<|<=|=|!=) (?P<value>.+)",
            "find_column": r"find (?P<field>\w+)",  
            

            
            # Aggregation Patterns
            "aggregate_sum": r"total (?P<field>\w+) by (?P<group>\w+)",
            "aggregate_count": r"count by (?P<group>\w+)",
            "aggregate_avg": r"average (?P<field>\w+) by (?P<group>\w+)",

            # Group By
            "group_by": r"group (?P<entity>\w+) by (?P<group>\w+)",

            # Sort
            "sort": r"sort (?P<entity>\w+) by (?P<field>\w+) (?P<order>ascending|descending)",

            # Having
            "having": r"having (?P<aggregate_type>total|average) (?P<field>\w+) (?P<operator>>|>=|<|<=|=|!=) (?P<value>\d+) grouped by (?P<group>\w+)",

            # Join
            "join": r"join (?P<from_collection>\w+) with (?P<to_collection>\w+) on (?P<local_field>\w+) as (?P<as_field>\w+)"
            
        }
        
        
    def parse_input(self, user_input):
        """Parse user input and return intent and parameters"""
        #user_input = user_input.lower().strip()
        user_input = user_input.strip()
        
        # MongoDB operator mapping
        operator_mapping = {
            ">": "gt",
            ">=": "gte",
            "<": "lt",
            "<=": "lte",
            "=": "eq",
            "!=": "ne"
        }
        
        for intent, pattern in self.patterns.items():
            match = re.search(pattern, user_input)
            if match:
                params = match.groupdict()
                
                if "operator" in params:
                    params["operator"] = operator_mapping.get(params["operator"], params["operator"])
                
                return {"intent": intent, "params": params}
            
        return {"intent": "unknown", "params": {}}