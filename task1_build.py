from pymongo import MongoClient
import json
import sys
import time
import utils


def connection(port):
    # Function to establish connection with MongoDB server
    client = MongoClient('localhost', port)
    database = client['MP2Norm']
    return database

def create_collection(database, collection_name):
    # Check if the collection already exists in the database
    # If the connection exists, drop it.
    if collection_name in database.list_collection_names():
        database[collection_name].drop()


def insert_data(database, collection_name, file_name, batch_size):
    # Function to insert data from a JSON file into a MongoDB collection
    with open(file_name, 'r', encoding='utf-8') as file:
        batch = []
        line_number = 0
        batch_num = 0
        # read in the file from the generator
        for document in utils.read_documents(file):
            batch.append(document)
            line_number += 1

            if line_number == batch_size:
                line_number = 0
                database[collection_name].insert_many(batch)
                batch = []
                batch_num += 1
                print("SAVING BATCH NUMBER {}".format(batch_num) )
        # remember: after the loop ends, the left-over items in the batch also needs to be inserted.
        if ( len(batch) > 0):
            database[collection_name].insert_many(batch)


def main(port):
    database = connection(port)

    # Read and insert messages
    start_time = time.time()
    create_collection(database, 'messages')
    insert_data(database, 'messages', 'messages.json', utils.BATCH_SIZE)  #
    print('Reading and inserting messages took {} seconds'.format(time.time() - start_time))

    # Read and insert sender
    start_time = time.time()
    create_collection(database, 'senders')
    insert_data(database, 'senders', 'senders.json', utils.BATCH_SIZE)
    print('Reading and inserting senders took {} seconds'.format(time.time() - start_time))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Not enough arguments, check if you have entered MongoDB port number.")
        exit()

    # Read port and connect to MongoDB server
    mdb_port = int(sys.argv[1])

    main(mdb_port)
