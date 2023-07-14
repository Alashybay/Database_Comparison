import redis
import time

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Define the number of iterations
num_iterations = 31
execution_times = []

# Execute the code multiple times
for i in range(num_iterations):
    start_time = time.time()

    # Your existing code here
    # Step 1: Fetch all book IDs
    book_ids = []
    books = r.keys("book:*")
    for book_key in books:
        book = r.hgetall(book_key)
        if b'book_id' in book:
            book_id = book[b'book_id'].decode('utf-8')
            book_ids.append(book_id)

    # Step 2: Filter books by author and borrowing year
    for book_id in book_ids:
        book_key = f"book:{book_id}"
        book = r.hgetall(book_key)
        if book.get(b'author', b'').decode('utf-8') == 'Rebecca Patterson':
            borrowing_key = f"borrowing_history:{book_id}"
            borrowings = r.smembers(borrowing_key)
            for borrowing in borrowings:
                borrow_data = r.hgetall(borrowing)
                borrow_date = borrow_data.get(b'borrow_date', b'').decode('utf-8')
                if borrow_date.startswith('2022'):
                    result = {
                        'title': book.get(b'title', b'').decode('utf-8'),
                        'author': book.get(b'author', b'').decode('utf-8'),
                        'borrow_date': borrow_date
                    }
                    print(result)

    # Step 3: Compute average borrows per book
    borrowing_key = f"borrowing_history:{book_id}"
    borrowings = r.smembers(borrowing_key)
    borrow_count = len(borrowings)
    avg_borrows_per_book = sum(len(r.smembers(f"borrowing_history:{other_book_id}")) for other_book_id in book_ids) / len(book_ids)

    # Step 4: Filter books with borrow count greater than average
    if borrow_count > avg_borrows_per_book:
        result = {
            'title': book.get(b'title', b'').decode('utf-8'),
            'author': book.get(b'author', b'').decode('utf-8'),
            'borrow_date': borrow_date
        }
        print(result)

    end_time = time.time()
    execution_time = end_time - start_time
    execution_times.append(execution_time)

# Write execution times to a file
with open('runtime250k_q4.txt', 'w') as f:
    for i, time in enumerate(execution_times, 1):
        f.write(f"{i+1},{time:.4f} sec\n")

r.close()
