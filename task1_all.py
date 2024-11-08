import sys
import task1_build
import task1_query

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Not enough arguments, check if you have entered MongoDB port number.")
        exit()

    # Read port and connect to MongoDB server
    mdb_port = int(sys.argv[1])

    # build the database
    task1_build.main(mdb_port)
    # run the queries
    task1_query.main(mdb_port)
