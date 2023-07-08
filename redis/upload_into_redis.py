import csv
import redis

# Connect to Redis container
r = redis.Redis(host='localhost', port=6379, db=0)
print('Connected to Redis container!')

# 0 -> 250k done
# 1 -> 500k need to do
# 2 -> 750k need to do
# 3 -> 1M need to do

# Insert data from books.csv into Redis
with open('/Users/kila/Desktop/books.csv', 'r') as file:
    csv_data = csv.DictReader(file)
    for row in csv_data:
        book_id = row['book_id']
        title = row['title']
        author = row['author']
        publication_date = row['publication_date']

        # Store book data in Redis hash
        book_key = f'book:{book_id}'
        r.hset(book_key, 'title', title)
        r.hset(book_key, 'author', author)
        r.hset(book_key, 'publication_date', publication_date)
        r.sadd('book', book_key)  # Add book key to the book key set

print('Data inserted into Redis for books!')

# Insert data from borrowers.csv into Redis
with open('/Users/kila/Desktop/borrowers.csv', 'r') as file:
    csv_data = csv.DictReader(file)
    for row in csv_data:
        borrower_id = row['borrower_id']
        name = row['name']
        email = row['email']

        # Store borrower data in Redis hash
        borrower_key = f'borrower:{borrower_id}'
        r.hset(borrower_key, 'name', name)
        r.hset(borrower_key, 'email', email)
        r.sadd('borrowers', borrower_key)  # Add borrower key to the borrowers key set

print('Data inserted into Redis for borrowers!')

# Insert data from borrowing_history.csv into Redis
with open('/Users/kila/Desktop/borrowing_history.csv', 'r') as file:
    csv_data = csv.DictReader(file)
    for row in csv_data:
        borrowing_id = row['borrowing_id']
        book_id = row['book_id']
        borrower_id = row['borrower_id']
        borrow_date = row['borrow_date']
        return_date = row['return_date']

        # Store borrowing history data in Redis hash
        borrowing_key = f'borrowing_history:{borrowing_id}'
        r.hset(borrowing_key, 'book_id', book_id)
        r.hset(borrowing_key, 'borrower_id', borrower_id)
        r.hset(borrowing_key, 'borrow_date', borrow_date)
        r.hset(borrowing_key, 'return_date', return_date)
        r.sadd('borrowing_history', borrowing_key)  # Add borrowing key to the borrowing_history key set

print('Data inserted into Redis for borrowing history!')

# Close the Redis connection
r.close()
