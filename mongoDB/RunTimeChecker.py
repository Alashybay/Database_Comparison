import pymongo
import time
import os

# Connect to MongoDB and specify the database
client = pymongo.MongoClient("mongodb://localhost:27018")
try:
    client.server_info()  # Check connection by accessing server info
except pymongo.errors.ConnectionFailure:
    print("Failed to connect to MongoDB.")
    exit()
print("Successfully connected to MongoDB.")

db = client["library1m"]
print("Successfully connected to the database.")

# Define the aggregation pipeline stages
pipeline = [
    {
        "$lookup": {
            "from": "borrowing_history",
            "localField": "book_id",
            "foreignField": "book_id",
            "as": "borrowing_history"
        }
    },
    {
        "$match": {
            "borrowing_history.borrow_year": 2022,
            "author": "Rebecca Patterson"
        }
    },
    {
        "$group": {
            "_id": "$_id",
            "title": {"$first": "$title"},
            "author": {"$first": "$author"},
            "borrow_date": {"$first": "$borrowing_history.borrow_date"}
        }
    },
    {
        "$sort": {"borrow_date": -1}
    }
]

# Execute the aggregation query 30 times and save the results in a file
with open("runtime1m_q4.txt", "w") as file:
    for i in range(1, 31):
        start_time = time.time()
        result = db.books.aggregate(pipeline)
        end_time = time.time()

        # Calculate the execution time
        execution_time = end_time - start_time

        # Write the execution time to the file
        file.write(f"{i}. {execution_time:.3f} sec\n")
    
client.close()
print("Successfully disconnected from MongoDB.")