from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

# Connect to Cassandra
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()
print('Successfully connected to Cassandra.')

# Create keyspace
session.execute("CREATE KEYSPACE IF NOT EXISTS library_1m WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

# Use keyspace
session.set_keyspace("library_1m")

# Create books table
session.execute("CREATE TABLE IF NOT EXISTS books (book_id INT PRIMARY KEY, title TEXT, author TEXT, publication_date DATE)")
print('Successfully created books table.')

# Create borrowers table
session.execute("CREATE TABLE IF NOT EXISTS borrowers (borrower_id INT PRIMARY KEY, name TEXT, email TEXT)")
print('Successfully created borrowers table.')

# Create borrowing_history table
session.execute("CREATE TABLE IF NOT EXISTS borrowing_history (borrowing_id INT PRIMARY KEY, book_id INT, borrower_id INT, borrow_date DATE, return_date DATE)")
print('Successfully created borrowing_history table.')

# Read books data from CSV and insert into books table
with open('/Users/kila/Desktop/books.csv', 'r') as file:
    next(file)  # Skip header row
    for line in file:
        book_id, title, author, publication_date = line.strip().split(',')
        session.execute(
            """
            INSERT INTO books (book_id, title, author, publication_date)
            VALUES (%s, %s, %s, %s)
            """,
            (int(book_id), title, author, publication_date)
        )
print('Successfully inserted books data.')

# Read borrowers data from CSV and insert into borrowers table
with open('/Users/kila/Desktop/borrowers.csv', 'r') as file:
    next(file)  # Skip header row
    for line in file:
        borrower_id, name, email = line.strip().split(',')
        session.execute(
            """
            INSERT INTO borrowers (borrower_id, name, email)
            VALUES (%s, %s, %s)
            """,
            (int(borrower_id), name, email)
        )
print('Successfully inserted borrowers data.')

# Read borrowing_history data from CSV and insert into borrowing_history table
with open('/Users/kila/Desktop/borrowing_history.csv', 'r') as file:
    next(file)  # Skip header row
    batch = BatchStatement()
    for line in file:
        borrowing_id, book_id, borrower_id, borrow_date, return_date = line.strip().split(',')
        batch.add(
            """
            INSERT INTO borrowing_history (borrowing_id, book_id, borrower_id, borrow_date, return_date)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (int(borrowing_id), int(book_id), int(borrower_id), borrow_date, return_date)
        )
        if len(batch) >= 100:  # Execute batch every 100 statements
            session.execute(batch)
            batch = BatchStatement()

    if len(batch) > 0:  # Execute remaining statements in the batch
        session.execute(batch)
print('Successfully inserted borrowing_history data.')

# Close the connection
cluster.shutdown()
print('Successfully closed connection to Cassandra.')