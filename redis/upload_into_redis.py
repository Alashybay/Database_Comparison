import csv
import redis

# Connect to Redis container
r = redis.Redis(host='localhost', port=6379, db=0)  # Specify db=0 for the default database

# Insert data from books.csv into the "1m" database
r.select(0)  # Select the default database (0)
with open('/Users/kila/Desktop/books.csv', 'r') as file:
    csv_data = csv.DictReader(file)
    for row in csv_data:
        book_id = row['book_id']
        title = row['title']
        author = row['author']
        publication_date = row['publication_date']

        # Store book data in Redis hash
        r.hset('book:' + str(book_id), 'title', title)
        r.hset('book:' + str(book_id), 'author', author)
        r.hset('book:' + str(book_id), 'publication_date', publication_date)

print('Data inserted into Redis "_" database for books!')

r.select(0)
with open('/Users/kila/Desktop/borrowes.csv', 'r') as file:
    csv_data = csv.DictReader(file)
    for row in csv_data:
        borrower_id = row['borrower_id']
        name = row['name']
        email = row['email']

        # Store borrower data in Redis hash
        r.hset('borrower:' + str(borrower_id), 'name', name)
        r.hset('borrower:' + str(borrower_id), 'email', email)

print('Data inserted into Redis "_" database for borrowers!')

r.select(0)
with open('/Users/kila/Desktop/borrowing_history.csv', 'r') as file:
    csv_data = csv.DictReader(file)
    for row in csv_data:
        borrowing_id = row['borrowing_id']
        book_id = row['book_id']
        borrower_id = row['borrower_id']
        borrow_date = row['borrow_date']
        return_date = row['return_date']

        # Store borrowing history data in Redis hash
        r.hset('borrowing_history:' + str(borrowing_id), 'book_id', book_id)
        r.hset('borrowing_history:' + str(borrowing_id), 'borrower_id', borrower_id)
        r.hset('borrowing_history:' + str(borrowing_id), 'borrow_date', borrow_date)
        r.hset('borrowing_history:' + str(borrowing_id), 'return_date', return_date)
print('Data inserted into Redis "_" database for borrowing history!')

# close the connection to the database
r.commit()
r.close()
