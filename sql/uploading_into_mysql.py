import csv
import pymysql

# Connect to MySQL database
conn = pymysql.connect(host='localhost', user='root', password='', db='library1m')
cursor = conn.cursor()
print("Connected to MySQL database!")

# Create temporary database
cursor.execute("CREATE DATABASE IF NOT EXISTS library1m")
cursor.execute("USE library1m")

# Create books table
cursor.execute('''CREATE TABLE IF NOT EXISTS books (
                    book_id INT PRIMARY KEY,
                    title VARCHAR(255),
                    author VARCHAR(255),
                    publication_date DATE
                )''')

# Create borrowers table
cursor.execute('''CREATE TABLE IF NOT EXISTS borrowers (
                    borrower_id INT PRIMARY KEY,
                    name VARCHAR(255),
                    email VARCHAR(255)
                )''')

# Create borrowing_history table
cursor.execute('''CREATE TABLE IF NOT EXISTS borrowing_history (
                    borrowing_id INT PRIMARY KEY,
                    book_id INT,
                    borrower_id INT,
                    borrow_date DATE,
                    return_date DATE,
                    FOREIGN KEY (book_id) REFERENCES books(book_id),
                    FOREIGN KEY (borrower_id) REFERENCES borrowers(borrower_id)
                )''')

# Insert data from books.csv
with open('/Users/kila/Desktop/books.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        book_id, title, author, publication_date = row
        query = f"INSERT INTO books (book_id, title, author, publication_date) VALUES ({book_id}, '{title}', '{author}', '{publication_date}')"
        cursor.execute(query)
print('Data inserted into books table!')

# Insert data from borrowers.csv
with open('/Users/kila/Desktop/borrowers.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        borrower_id, name, email = row
        query = f"INSERT INTO borrowers (borrower_id, name, email) VALUES ({borrower_id}, '{name}', '{email}')"
        cursor.execute(query)
print('Data inserted into borrowers table!')

# Insert data from borrowing_history.csv
with open('/Users/kila/Desktop/borrowing_history.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip header row
    for row in csv_data:
        borrowing_id, book_id, borrower_id, borrow_date, return_date = row
        query = f"INSERT INTO borrowing_history (borrowing_id, book_id, borrower_id, borrow_date, return_date) VALUES ({borrowing_id}, {book_id}, {borrower_id}, '{borrow_date}', '{return_date}')"
        cursor.execute(query)
print('Data inserted into borrowing_history table!')

# Commit the changes and close the connection
conn.commit()
print('Changes committed!')
conn.close()
print('Connection closed!')
