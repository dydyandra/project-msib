from collections import defaultdict

from attr import field
from mongotypes import get_type_string, common_parent
from pymongo import errors

def extract_schema_from_client(client, database, collection, database_names=None):
    """
    Function to get list of database names inside schema and matching it with the defined database. 
    """

    # 1. Get list of database names
    if database_names is None:
        database_names = client.list_database_names()

    # 2. Call extract_schema_database for database and collection defined. Output will be in dictionary format. 
        mongoschema = dict()

        databasename = client[database]
        list_of_collections = databasename.list_collection_names()
        try:
            if collection in list_of_collections:
                database_schema = extract_schema_database(databasename,  collection)
                mongoschema = database_schema
            else: 
                print(f"Collection {collection} doesn't exist.")

        except Exception as e: 
            # print(e)
            print("Database doesn't exist.")

        return mongoschema


def extract_schema_database(databasename, collection_name):
    """
    Function to list all collection names and match it with the defined collection. 
    """

    # 3. List collection names from the database defined
    collections_from_database = databasename.list_collection_names()

    # 4. If collection defined is inside list of collection, function to extract fields will be called. Output will be in dictionary format. 
    database_schema = dict()
    pymongo_collection = databasename[collection_name]

    database_schema = extract_collection_schema(pymongo_collection)
    
    return database_schema 


def extract_collection_schema(pymongo_collection):
    """
    Function to extract fields inside the collection defined. 
    """

    # 5. Initialize dictionary for collection. If collection is typed Object --> empty object will be initialized. 
    collection_schema = {
        'count' : 0,
        "field": init_empty_object_schema()
    }

    # 6. Get all documents from collection. 
    documents = pymongo_collection.find({})
    

    # 7. Add all documents to schema --> function will be used to analyze data type, fields available in document
    for document in documents:
        add_document_to_object_schema(document, collection_schema['field'])


    # 16. Processing schema
    post_process_schema(collection_schema)


    # 26. Return schema in dictionary form
    collection_schema = recursive_default_to_regular_dict(collection_schema)
    return collection_schema

def recursive_default_to_regular_dict(value):
    if isinstance(value, dict):
        d = {k: recursive_default_to_regular_dict(v) for k, v in value.items()}
        return d
    else:
        return value

def post_process_schema(object_count_schema):
    """
    Function to get total of fields and subfields and clean list of types taken for each field/subfields. 
    """
    object_count = object_count_schema['count']
    object_schema = object_count_schema['field']
    object_count = 0; # initialize count first 

    # 17. iterate through each field. Count will be incremented for loop. 
    for field_schema in object_schema.values():
        object_count += 1

        #18. clean list for each field
        clean_types(field_schema)

        if 'field' in field_schema:
            # 22. iterate through the subfields if exist
            post_process_schema(field_schema)

    # 24. Total of field will be put in key 'count' 
    object_count_schema['count'] = object_count
    for field_schema in object_schema.values():

        ## 25. if count is 0 --> field has no subfields, key will be removed. 
        if field_schema['count'] == 0:
            field_schema.pop('count')

def clean_types(field_schema):
    """
    Function to clean list of data types. For example, if data type has ARRAY (not needed, as array has been checked for data type values)
    """
    type_list = list(field_schema['type'])
    cleaned_type_list = [type_name for type_name in type_list
                        if type_name!='ARRAY' and type_name != 'NULL']

    # 19. Change list into only strings (if only 1 data type)
    common_type = common_parent(cleaned_type_list)


    # 20. If ARRAY is in type,
    if 'ARRAY' in field_schema['type']:

        #21. if in array there's no subfields, pop key 'field' from dictionary
        if not field_schema['field']:
            field_schema.pop('field')
            field_schema['type'] = common_type

            #22. to indicate whether array/not if not array of object
            field_schema['isarray'] = 'TRUE'
        else:
            #23. to indicate whether array/not
            field_schema['isarray'] = field_schema.pop('type')
            field_schema['isarray'] = 'TRUE'

    else:
        field_schema['type'] = common_type
    

def init_empty_object_schema():
    """
    Function to initialize empty dictionary for object. 
    """
    def empty_field_schema():
        field_dict = {
            'type' : [], 
            'count' : 0,
        }
        return field_dict

    empty_object = defaultdict(empty_field_schema)
    return empty_object


def add_document_to_object_schema(document, object_schema):
    """
    Function to list all data types inside document for each field, and analyze whether there are subfields/sub-elements inside each field. 
    """

    # 8. For each field in document, data types will be listed
    for field, value in document.items():
        add_value_type(value, object_schema[field]) 

        # 10. Check whether in document, field has subfields or elements. If yes, function will be called to analyze the subfields. 
        if isinstance(value, (dict, list)):
            add_potential_document_to_field_schema(value, object_schema[field])
        

def add_potential_document_to_field_schema(document, field_schema):
    """
    Function to analyze all potential subfields and sub-elements inside a field.
    """

    # 11. Initialize empty object. 
    if 'field' not in field_schema:
        field_schema['field'] = init_empty_object_schema() 

    # 12. If subfield is of type OBJECT in mongodb, previous function will be called as type is recognized as a field. 
    if isinstance(document, dict):                          
        add_document_to_object_schema(document, field_schema['field'])

    # 13. If subfield is of type ARRAY in mongodb, each value in array will be checked. 
    if isinstance(document, list):               
        for value in document:

            #14. if value is of type OBJECT in mongodb which means in array there are more fields to be checked. 
            if isinstance(value, dict):
                add_document_to_object_schema(value, field_schema['field'])
            else:
                #15. if value is not type OBJECT and instead is just a normal array with no more subfields. 
                add_value_type(value, field_schema)
        
def add_value_type(value, field_schema):
    """
    Function to list all unique value types inside a field in a document in list form. See mongotypes.py for function explanation. 
    """
    
    value_type_str = get_type_string(value) # 9. Change mongodb data types to common data types. 

    if value_type_str not in field_schema['type']:
        field_schema['type'].append(value_type_str)

