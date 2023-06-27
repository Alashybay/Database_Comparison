import csv
import random
import os

from faker import Faker

fake = Faker()

# Set the path to the desktop directory
desktop_path = os.path.expanduser("~/Desktop")
print('Desktop path: ', desktop_path)

# Generate unique primary keys
book_id_counter = 1
borrower_id_counter = 1
borrowing_id_counter = 1

# Generate data and save to CSV files
with open(os.path.join(desktop_path, 'books.csv'), 'w', newline='') as books_file, \
        open(os.path.join(desktop_path, 'borrowers.csv'), 'w', newline='') as borrowers_file, \
        open(os.path.join(desktop_path, 'borrowing_history.csv'), 'w', newline='') as borrowing_history_file:

    books_writer = csv.writer(books_file)
    borrowers_writer = csv.writer(borrowers_file)
    borrowing_history_writer = csv.writer(borrowing_history_file)

    # Write headers to CSV files
    books_writer.writerow(['book_id', 'title', 'author', 'publication_date'])
    borrowers_writer.writerow(['borrower_id', 'name', 'email'])
    borrowing_history_writer.writerow(['borrowing_id', 'book_id', 'borrower_id', 'borrow_date', 'return_date'])
    print('Headers written to CSV files')

    # Generate and save book data
    for _ in range(1000000):
        title = fake.catch_phrase()
        author = fake.name()
        publication_date = fake.date_between(start_date='-10y', end_date='today')
        books_writer.writerow([book_id_counter, title, author, publication_date])
        book_id_counter += 1
    print('Book data generated and saved to CSV file')

    # Generate and save borrower data
    for _ in range(950000):
        name = fake.name()
        email = fake.email()
        borrowers_writer.writerow([borrower_id_counter, name, email])
        borrower_id_counter += 1
    print('Borrower data generated and saved to CSV file')

    # Generate and save borrowing history data
    for _ in range(900000):
        book_id = random.randint(1, 900000)
        borrower_id = random.randint(1, 900000)
        borrow_date = fake.date_between(start_date='-1y', end_date='today')
        return_date = fake.date_between(start_date=borrow_date, end_date='today')
        borrowing_history_writer.writerow([borrowing_id_counter, book_id, borrower_id, borrow_date, return_date])
        borrowing_id_counter += 1
    print('Borrowing history data generated and saved to CSV file')