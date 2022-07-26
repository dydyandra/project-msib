import bson

# Mapping from pymongo_type to type_string
PYMONGO_TYPE_TO_TYPE_STRING = {
    
    list: "ARRAY",
    dict: "OBJECT",
    type(None): "NULL",
    bool: "BOOLEAN",
    int: "INTEGER",
    bson.int64.Int64: "BIGINTEGER",
    bson.decimal128.Decimal128: "FLOAT",
    float: "FLOAT",
    str: "STRING",
    bson.datetime.datetime: "DATE",
    bson.timestamp.Timestamp: "TIMESTAMP",
    bson.dbref.DBRef: "DBREF",
    bson.objectid.ObjectId: "OID",
}

try:
    PYMONGO_TYPE_TO_TYPE_STRING[str] = 'STRING'
except NameError:
    pass

def get_type_string(value):
    """
    Function to change types from MongoDB into common type in string form. 
    """
    value_type = type(value)
    try:
        type_string = PYMONGO_TYPE_TO_TYPE_STRING[value_type]
    except KeyError:
        PYMONGO_TYPE_TO_TYPE_STRING[value_type] = 'unknown'
        type_string = 'unknown'

    return type_string


def common_parent(list_of_type_string):
    if not list_of_type_string:
        return 'null'
   
    list_of_type_string = list(set(list_of_type_string))
    if len(list_of_type_string) == 1:
        return list_of_type_string[0]
    return list_of_type_string


