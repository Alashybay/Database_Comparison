import redis
import time

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

def getAllBooks():
    # get all the book keys
    keys = r.keys('book:*')

    # retrieve values for each book key
    for key in keys:
        book = r.hvals(key)
        print(book)

    return books

getAllBooks()

# Close the Redis connection
r.close()
