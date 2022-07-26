# MongoDB Analyzer
## Introduction
MongoDB is an open source cross-platform document-oriented database program. As it is classified as a NoSQL database program in which the database is non-tabular and not structured, MongoDB uses JSON-like documents with optional schemas. One of the many advantages of using MongoDB is the flexibility of the schema. As the schema is not predefined, it has a dynamic schematic architecture that works with non-structured data and storage. MongoDB is useful when business and the data they maintain keeps evolving, so having a flexible database model that could adapt is important.

As the schema is dynamic, and the documents inside the schema is many, processing the data could be tedious as number of fields and content can differ for each documents. There are a possibility of a field having multiple sub-fields also, and sub-fields inside an array, that only appears in some documents. This makes checking and analyzing the schema harder. There should be away to flatten the schema as well as the fields are sometimes in a hierarchical structure and to be able to process the data efficiently, all fields should be counted and recorded. 

Schema Analyzer is a python-based program created to analyze and flatten the schema of a collection and database defined and to solve the problems above. It uses pymongo to access the MongoDB data, and the output will be a json-type file consisting of a dictionary filled with the total of fields inside a document along with the list of fields and data types of each field. Sub-fields will also be analyzed.


## Implementation

### Requirements
MongoDB Analyzer requires the following to run:
- Python version 3.91.1
- pymongo==4.1.1


### How To Use
Before running the program, host, database and collection will need to be defined first in the main.py. If run on server, please define the URI or host, username and password depending on which type of connection will be used. 

Run the main.py program. Output will be saved on a separate folder called 'output', with the title of the file being the database and collection name that has been defined for the program. 

### Output
Output will be a dictionary of fields in json format. Example of the output is as follow: 

```json
{
    "count": 11,
    "field": {
        "_id": {
            "type": [
                "OID",
                "INTEGER"
            ]
        },
        "title": {
            "type": "STRING"
        },
        "isbn": {
            "type": "STRING"
        },
        "pageCount": {
            "type": "INTEGER"
        },
        "publishedDate": {
            "type": "DATE"
        },
        "thumbnailUrl": {
            "type": "STRING"
        },
        "shortDescription": {
            "type": "STRING"
        },
        "longDescription": {
            "type": "STRING"
        },
        "status": {
            "type": "STRING"
        },
        "authors": {
            "type": [
                "STRING",
                "FLOAT"
            ]
        },
        "categories": {
            "type": "STRING"
        }
    }
}
```
As seen, some of the insights inside the output are: 
1. The total of fields (parent field and also subfield if exist)
2. Data type of each field. If there are multiple data types, all will appear in list form. 
3. Name of the fields

There are some types of output.

#### Nested Object
When field/object has more subfields inside of it. The output will be nested similar to the one above. 
```json
{
    "count":   ,
    "field": {
        "images": {
            "type": "OBJECT",
            "count": 4,
            "field": {
                "thumbnail_url": {
                    "type": "STRING"
                },
                "medium_url": {
                    "type": "STRING"
                },
                "picture_url": {
                    "type": "STRING"
                },
                "xl_picture_url": {
                    "type": "STRING"
                }
            }
        }
    }
}
```

#### Array of Object
When a field in MongoDB has the field type of 'ARRAY' (also shown as elements) and in array has several objects or fields that needed to be stated. Example of output is as below:

```json
{
    "count":   ,
    "field": {
        "address": {
            "type": "OBJECT",
            "count": 7,
            "field": {
                "street": {
                    "type": "STRING"
                },
                "suburb": {
                    "type": "STRING"
                },
                "government_area": {
                    "type": "STRING"
                },
                "market": {
                    "type": "STRING"
                },
                "country": {
                    "type": "STRING"
                },
                "country_code": {
                    "type": "STRING"
                },
                "location": {
                    "type": "OBJECT",
                    "count": 3,
                    "field": {
                        "type": {
                            "type": "STRING"
                        },
                        "coordinates": {
                            "count": 1,
                            "field": {
                                "$numberDouble": {
                                    "type": "STRING"
                                }
                            },
                            "isarray": "TRUE"
                        },
                        "is_location_exact": {
                            "type": "BOOLEAN"
                        }
                    }
                }
            }
        },

    }
}
```

In the example above, the nested dictionary is more complex as there are multiple nested fields. To indicate whether the field is an array, the key 'isarray' is used: 
```json
                        "coordinates": {
                            "count": 1,
                            "field": {
                                "$numberDouble": {
                                    "type": "STRING"
                                }
                            },
                            "isarray": "TRUE"
                        },
```


#### Array Only
When a field is in type ARRAY but does not have any subfield or objects inside it again: 
```json
        "authors": {
            "type": [
                "BIGINTEGER",
                "STRING"
            ],
            "isarray": "TRUE"
        },
        "categories": {
            "type": "STRING",
            "isarray": "TRUE"
        }
```
The key 'isarray' is also used to indicate whether the field is an array/not. 

### Code
There are three files used for the program: 
1. `main.py` - Main program to run and to create file. 
2. `extract.py` - Program to extract the collection from database. Result will be returned to main to be saved into a file. 
3. `mongotypes.py` - Program to check data types of each field in collection. 


#### main.py
List of function used: 
1. `main` -  Main function to run, from getting client via MongoDB, extracting, calling output and show execution time. 
2. `output_schema` - Function to put output inside a file in a folder. Output will be saved into json, with the tile of database name and collection name. 

#### extract.py
List of function used: 
1. `extract_schema_from_client` -  Function to get list of database names inside schema and matching it with the defined database. 
2. `extract_schema_database` - Function to list all collection names and match it with the defined collection. 
3. `extract_collection_schema` - Function to extract fields inside the collection defined. 
4. `recursive_default_to_regular_dict` = Function to turn processed schema into dictionary
5. `post_process_schema` - Function to get total of fields and subfields and clean list of types taken for each field/subfields. 
6. `clean_types` - Function to clean list of data types. For example, if data type has ARRAY (not needed, as array has been checked for data type values)
7. `init_empty_object_schema` - Function to initialize empty dictionary for object. 
8. `add_document_to_object_schema` - Function to list all data types inside document for each field, and analyze whether there are subfields/sub-elements inside each field. 
9. `add_potential_document_to_field_schema` - Function to analyze all potential subfields and sub-elements inside a field.
10. `add_value_type` - Function to list all unique value types inside a field in a document in list form. See mongotypes.py for function explanation. 


#### mongotypes.py
List of functions used: 
1. `get_type_string` -  Function to change types from MongoDB into common type in string form. 
2. `common_parent` - Function to get singular data type if one only exists. 






