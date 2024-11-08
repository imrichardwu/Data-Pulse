import pymongo
from pymongo import MongoClient
from pymongo.errors import ExecutionTimeout
import time
import sys
import utils

def query_q1(db, use_search):
    # Return the number of messages that have “ant” in their text.
    # this is solved with regex.
    try:
        start_time = time.time()
        # search is only applicable for indexed data!
        if (use_search):
            qry = {"$text": {"$search": "*ant"}}
        else:
            qry = {"text": {"$regex": "ant"}}
        result = db['messages'].count_documents(qry, 
                                                maxTimeMS=utils.TIME_LIMIT_TWO_MINUTES_MS)
        end_time = time.time()
        print("### Q1 (method: {}) ###".format("search" if use_search else "regex"))
        print("Output: {}".format(result))
        print("Time: {} s | {} ms".format((end_time - start_time), (end_time - start_time)*1000))
    except ExecutionTimeout:
        print("Q1: Query took more than two minutes.")


def query_q2(db):
    # Find the nick name/phone number of the sender who has sent the greatest number of messages.
    # Return the nick name/phone number and the number of messages sent by that sender.
    # You do not need to return the senders name or credit.
    # this is solved with grouping as the nickname would be enough.
    try:
        start_time = time.time()
        result = db['messages'].aggregate([
            {"$group": {"_id": "$sender", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ], maxTimeMS=utils.TIME_LIMIT_TWO_MINUTES_MS)
        end_time = time.time()
        print("### Q2 ###")
        print("Query output: {}".format([i for i in result]))
        print("Time: {} s | {} ms".format((end_time - start_time), (end_time - start_time)*1000))
    except ExecutionTimeout:
        print("Q2: Query took more than two minutes.")


def query_q3(db):
    # Return the number of messages where the sender’s credit is 0.
    # This would require filtering out the senders with zero credit
    # then lookup into the messages for those senders, finally count the number of messages.
    try:
        start_time = time.time()
        result = list(db['senders'].aggregate([
            {"$match": {"credit": 0}},  # Find senders with zero credit
            {"$lookup": {  # Perform a lookup to count messages
                "from": "messages",
                "localField": "sender_id",
                "foreignField": "sender",
                "as": "messages"
            }},
            {"$unwind": "$messages"},
            {"$count": "message_count"}  # Sum up the total messages
        ], maxTimeMS=utils.TIME_LIMIT_TWO_MINUTES_MS))
        end_time = time.time()
        print("### Q3 ###")
        print("Query output: {}".format(result))
        print("Time: {} s | {} ms".format((end_time - start_time), (end_time - start_time)*1000))
    except ExecutionTimeout:
        print("Q3: Query took more than two minutes.")


def query_q4(db):
    # Double the credit of all senders whose credit is less than 100.
    # This would require to multiply the credit by 2 where the credit is less than 100.
    try:
        start_time = time.time()
        result = db['senders'].update_many({"credit": {"$lt": 100}}, {"$mul": {"credit": 2}})
        end_time = time.time()
        print("### Q4 ###")
        print("Query output: {}".format(result))
        print("Time: {} s | {} ms".format((end_time - start_time), (end_time - start_time)*1000))

        # NOTE: the python time function is in seconds.
        if end_time - start_time > utils.TIME_LIMIT_TWO_MINUTES_SEC:
            raise ExecutionTimeout

    except ExecutionTimeout:
        print("Q4: Query took more than two minutes.")


def create_indices(db):
    # create the appropriate index based on value type
    res = db['messages'].create_index([("sender", pymongo.ASCENDING)])
    print("Created index \'{}\' for the \'messages\' collection".format(res))
    res = db['messages'].create_index([("text", pymongo.TEXT)])
    print("Created index \'{}\' for the \'messages\' collection".format(res))
    res = db['senders'].create_index([("sender_id", pymongo.ASCENDING)])
    print("Created index \'{}\' for the \'senders\' collection".format(res))


def main(port):
    # connect
    client = MongoClient('localhost', port)
    # get database
    database = client['MP2Norm']
    # drop exising indexes before the first half of testing
    database['messages'].drop_indexes()
    database['senders'].drop_indexes()

    # test how quickly queries run WITHOUT indexes
    query_q1(database, False)
    query_q2(database)
    query_q3(database)
    query_q4(database)
    # create the index
    create_indices(database)
    # test how quickly queries run WITH indexes
    # special note: the regex search with index is slower!
    query_q1(database, True)
    query_q1(database, False)
    query_q2(database)
    query_q3(database)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Not enough arguments, check if you have entered MongoDB port number.")
        exit()

    # Read port and connect to MongoDB server
    mdb_port = int(sys.argv[1])

    main(mdb_port)
