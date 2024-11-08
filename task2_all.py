import sys
import task2_build
import task2_query

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Not enough arguments, check if you have entered MongoDB port number.")
        exit()

    # Read port and connect to MongoDB server
    mdb_port = int(sys.argv[1])

    # Read the data from file
    task2_build.main(mdb_port)
    # Test the queries
    task2_query.main(mdb_port)
