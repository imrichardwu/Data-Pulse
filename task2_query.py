import ast

from pymongo import MongoClient, WriteConcern
from pymongo.errors import ExecutionTimeout, WriteConcernError
import json
import sys
import time
import utils

def query_q1(db):
    # Return the number of messages that have “ant” in their text.
    # this is solved with regex.
    # This solution is the same as task1 counterpart.
    try:
        start_time = time.time()
        result = db['messages'].count_documents({"text": {"$regex": "ant"}}, maxTimeMS=utils.TIME_LIMIT_TWO_MINUTES_MS)
        end_time = time.time()
        print("### Q1 ###")
        print(result)
        print("Time: {} s | {} ms".format((end_time - start_time),(end_time - start_time)*1000))
    except ExecutionTimeout:
        print("Q1: Query took more than two minutes.")

def query_q2(db):
    # Find the nick name/phone number of the sender who has sent the greatest number of messages.
    # Return the nick name/phone number and the number of messages sent by that sender.
    # You do not need to return the senders name or credit.
    # this is solved with grouping as the nickname would be enough.
    # This solution is the same as task1 counterpart.
    try:
        start_time = time.time()
        result = db['messages'].aggregate([
            {"$group": {"_id": "$sender", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ], maxTimeMS=utils.TIME_LIMIT_TWO_MINUTES_MS)
        sender_info = [i for i in result]
        end_time = time.time()
        print("### Q2 ###")
        print("Query output: {}".format(sender_info))
        print("Time: {} s | {} ms".format((end_time - start_time), (end_time - start_time)*1000))
    except ExecutionTimeout:
        print("Q2: Query took more than two minutes.")

def query_q3(db):
    # Return the number of messages where the sender’s credit is 0.
    # As the task2 grabbed the sender info during preprocessing, the solution would be a simple count with condition.
    # This solution is DIFFERENT FROM task1 counterpart.
    try:
        start_time = time.time()
        result = db['messages'].count_documents({"sender_info.credit": 0}, maxTimeMS=utils.TIME_LIMIT_TWO_MINUTES_MS)

        end_time = time.time()
        print("### Q3 ###")
        print("Query output: {}".format(result))
        print("Time: {} s | {} ms".format((end_time - start_time), (end_time - start_time)*1000))
    except ExecutionTimeout:
        print("Q3: Query took more than two minutes.")

def query_q4(db):
    # Double the credit of all senders whose credit is less than 100.
    # This would require to multiply the credit by 2 where the credit is less than 100.
    # This solution is the same as task1 counterpart.
    try:
        start_time = time.time()
        result = db['messages'].update_many({"sender_info.credit": {"$lt": 100}}, {"$mul": {"sender_info.credit": 2}})
        end_time = time.time()
        print("### Q4 ###")
        print("Query output: {}".format(result))
        print("Time: {} s | {} ms".format((end_time - start_time), (end_time - start_time)*1000))
        if end_time - start_time > utils.TIME_LIMIT_TWO_MINUTES_SEC:
            raise ExecutionTimeout

    except WriteConcernError:
        print("Q4: Query took more than two minutes.")

def main(port):
    client = MongoClient('localhost', port)
    database = client['MP2Embd']

    # test the queries
    query_q1(database)
    query_q2(database)
    query_q3(database)
    query_q4(database)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Not enough arguments, check if you have entered MongoDB port number.")
        exit()

    # Read port and connect to MongoDB server
    mdb_port = int(sys.argv[1])

    main(mdb_port)