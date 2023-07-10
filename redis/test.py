import redis
import time

# Connect to Redis
r = redis.Redis(host='localhost', port=6379)


# Retrieve book IDs for the specified year
start_timestamp = int(time.mktime(time.strptime(f'2023-01-01', '%Y-%m-%d')))
end_timestamp = int(time.mktime(time.strptime(f'2024-01-01', '%Y-%m-%d')))
book_ids = r.zrangebyscore('borrowing_history', start_timestamp, end_timestamp)

# Retrieve book details and borrower names

for book_id in book_ids:
    book_details = r.hgetall(f'book:{book_id}')
    borrower_id = r.hget('borrowing_history', f'borrowing:{book_id}:borrower_id')
    borrower_name = r.hget('borrowers', f'borrower:{borrower_id}:name')

    # print book title, book author, and borrower name
    print(f'{book_details[b"title"].decode("utf-8")} by {book_details[b"author"].decode("utf-8")}, borrowed by {borrower_name.decode("utf-8")}')

# Close the Redis connection
r.close()
