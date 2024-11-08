import ast

from pymongo import MongoClient, WriteConcern
from pymongo.errors import ExecutionTimeout, WriteConcernError
import json
import sys
import time
import utils



def connection(port):
    # Function to establish connection with MongoDB server
    client = MongoClient('localhost', port)
    database = client['MP2Embd']
    return database


def create_collection(database, collection_name):
    # Check if the collection already exists in the database
    # If the connection exists, drop it.
    if collection_name in database.list_collection_names():
        database[collection_name].drop()


def insert_data(database, collection_name, message,sender, batch_size):
    # Function to insert data from a JSON file into a MongoDB collection
    sender_dict = dict()
    with open(sender, 'r', encoding='utf-8') as file:
        for doc in utils.read_documents(file):
            if 'sender_id' in doc:
                doc_sender = doc['sender_id']
                if doc_sender in sender_dict:
                    print("WARN: duplicated document sender {} in the senders file.".format(doc_sender))
                # record the document sender information
                sender_dict[doc_sender] = doc

    # then, read the messages.
    with open(message, 'r', encoding='utf-8') as file:
        batch = []
        doc_number = 0
        batch_num = 0
        for document in utils.read_documents(file):
            # this section would address missing value/mismatch from sender info file
            # basically, any error that would possibly crash the program here.
            # note that invalid information are filled with None placeholder.
            if ("sender" not in document):
                print("WARN: the message has no sender information.")
                document["sender"] = None
                document["sender_info"] = None
            else:
                sender_name = document["sender"]
                if (sender_name not in sender_dict):
                    print("WARN: the message sender was not recorded in the senders file.")
                    document["sender_info"] = None
                else:
                    document["sender_info"] = sender_dict[sender_name]
            # after processing, append the document to the batch.
            batch.append(document)
            doc_number += 1

            # handle the batch once the size is reached
            if doc_number == batch_size:
                doc_number = 0
                database[collection_name].insert_many(batch)
                batch = []
                batch_num += 1
                print("SAVING BATCH NUMBER {}".format(batch_num) )
        # after the loop, insert the remaining documents in the batch
        if ( len(batch) > 0):
            database[collection_name].insert_many(batch)


def main(port):
    database = connection(port)

    # Read and insert messages
    start_time = time.time()
    create_collection(database, 'messages')
    insert_data(database, 'messages', 'messages.json','senders.json', utils.BATCH_SIZE)  
    print('Reading and inserting messages took {} seconds'.format(time.time() - start_time))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Not enough arguments, check if you have entered MongoDB port number.")
        exit()

    # Read port and connect to MongoDB server
    mdb_port = int(sys.argv[1])

    main(mdb_port)
    
