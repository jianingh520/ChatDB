import nosql_functions as nosql_func
from nlp_processor import NLPProcessor

class CollectionLayer:
    def __init__(self, database):
        self.database = database
        
    def list_colelctions(self):
        return self.database.list_collection_names()
    
    def start(self):
        while True:
            col_list = "\n".join(self.list_colelctions())
            print(
            f"\n--- Database: {self.database.name} is selected ---\n"
            f"Available collections:\n{col_list}\n"
            "\nTo proceed, choose an existing collection or create a new one with command: 'upload json'.\n"
            "Use 'exit' to return to the database page.\n")
            col_name = input("Enter collection name or upload json file to create collection (or 'exit' to go back): ").strip()
            col_name = col_name.lower()
            
            if col_name == 'exit':
                return None
            
            if col_name == 'mainmenu':
                return 'MAIN_MENU'
            
            if col_name in self.list_colelctions():
                return col_name
            
            if col_name == 'upload json':
                collection = input("Enter collection name: ")
                json_file = input("Enter the path of your json file: ")
                nosql_func.upload_JSON_data(self.database[collection], json_file)
                print(f'file: {json_file} uploaded in {collection}!\n')
                # continue
                return collection
            
            if col_name.startswith('create collection '):
                col_name = col_name.split()[2]
                collection = nosql_func.set_collection(self.database, col_name)
                print(f"database: {col_name} created!\n")
                continue
            
            print("Invalid collection. Please try again.")