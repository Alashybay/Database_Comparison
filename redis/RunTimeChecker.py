import redis
import time

# Connect to Redis database
r = redis.Redis(host='localhost', port=6379)

# Query: Retrieve books based on publication date
def query():
    matching_books = []
    for key in r.scan_iter(match='book:*:publication_date'):
        publication_date = r.hget(key, 'publication_date').decode('utf-8')
        if publication_date.startswith('2021'):
            book_id = key.decode('utf-8').split(':')[1]
            book_title = r.hget(f'book:{book_id}', 'title').decode('utf-8')
            book_author = r.hget(f'book:{book_id}', 'author').decode('utf-8')
            matching_books.append(f'{book_title}, {book_author}, {publication_date}')
    return matching_books

# Execute the query and record the execution times
execution_times = []
for _ in range(30):
    start_time = time.time()
    result = query()
    execution_time = time.time() - start_time
    execution_times.append(execution_time)

# Save the execution times in a text file
with open('redis_query1_execution_times.txt', 'w') as file:
    for i, time in enumerate(execution_times, 1):
        file.write(f'{i}. {time} sec\n')