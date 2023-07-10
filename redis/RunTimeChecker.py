import redis
import time

# Connect to Redis
r = redis.Redis(host='localhost', port=6379)

# Define the number of iterations
num_iterations = 30
count = 0
# Execute the code multiple times and record execution times
execution_times = []
for _ in range(num_iterations):
    start_time = time.time()

    # Retrieve books based on publication_date pattern
    for key in r.scan_iter(match='book:*'):
        book_data = r.hgetall(key)
        if book_data:
            publication_date = book_data.get(b'publication_date', b'')
            if publication_date.startswith(b'2021'):
                count += 1
                
    end_time = time.time()
    execution_time = end_time - start_time
    execution_times.append(execution_time)

# Save execution times in a text file
with open('redis_query_execution_times.txt', 'w') as file:
    for i, time in enumerate(execution_times, 1):
        file.write(f'{i}. {time:.4f} sec\n')

# Close the Redis connection
r.close()
