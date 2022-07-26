# coding: utf8

import sys
import json
import pymongo
import extract
import time
import os

# ##### WITH LOCAL
# host = 'localhost'
# port = 27017
# database = 'zips'
# collection = 'listingsAndReviews'

# def main():
#     st  = time.time()
#     client = pymongo.MongoClient(host, port)
#     extract_collection = extract.extract_schema_from_client(client, database, collection)
#     filename = database + '_' + collection
#     output_schema(extract_collection, filename)
#     et = time.time()
#     elapsed_time = et - st
#     print('Execution time:', elapsed_time, 'seconds')



# ##### WITH SERVER
database = ' '
collection = ' '

# ## with URI
uri = ' '

## with host
host = ' '
username = ' '
password = ' '

def main():
    """
    Main function to run, from getting client via MongoDB, extracting, calling output and show execution time. 
    """
    st  = time.time()

     # 1. get client via username and password or URI 
    # client = pymongo.MongoClient(host,
    #                     username=username,
    #                     password=password,
    #                     authSource=username,
    #                     authMechanism='SCRAM-SHA-1')

   
    client = pymongo.MongoClient(uri)

    # 2. Call function from extract.py. Output will be dictionary of the fields in collection
    extract_collection = extract.extract_schema_from_client(client, database, collection)

    filename = database + '_' + collection

    #3. Create file for output
    if extract_collection:
        output_schema(extract_collection, filename)


    et = time.time()
    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')


def output_schema(collection, filename=None):
    """
    Function to put output inside a file in a folder. Output will be saved into json, with the tile of database name and collection name. 
    """

    # 1. Creating folder if not exist
    cwd = os.getcwd()
    dir_path = cwd + '/output'
    os.makedirs(dir_path, exist_ok=True)

    if filename is None:
        output_file = sys.stdout
    else:
        if not filename.endswith('.' + 'json'):
            filename += '.' + 'json'
        dir_path = dir_path + '/' + filename

    # 2. Dump output inside a json file based on path created above
    with open(dir_path, 'w') as output_file:
        json.dump(collection, output_file, indent=4)


if __name__ == '__main__':
    main()