
from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('library_250k')

# Create the denormalized_data table
create_table_query = """
    CREATE TABLE IF NOT EXISTS denormalized_data (
        book_id INT PRIMARY KEY, 
        title TEXT, 
        author TEXT, 
        publication_date DATE,
        borrower_id INT,
        name TEXT, 
        email TEXT,
        borrowing_id INT,
        borrow_date DATE, 
        return_date DATE,
    );
"""
session.execute(create_table_query)

# Insert data into the denormalized_data table using separate queries
insert_books_query = "INSERT INTO denormalized_data (book_id, title, author, publication_date) SELECT book_id, title, author, publication_date FROM books;"
session.execute(insert_books_query)

insert_borrowing_his_query = "INSERT INTO denormalized_data (borrowing_id,borrower_id,borrow_date,return_date) SELECT borrowing_id,book_id,borrower_id,borrow_date,return_date FROM borrowing_history;"
session.execute(insert_borrowing_his_query)

insert_borrowers_query = "INSERT INTO denormalized_data (borrower_id,name,email) SELECT borrower_id,name,email FROM borrowers;"
session.execute(insert_borrowers_query)

# Perform a complex query on the denormalized_data table
query = """
    SELECT title, author, borrower_name
    FROM denormalized_data
    WHERE borrow_date >= '2023-01-01' AND borrow_date < '2024-01-01';
"""

# Execute the query and measure runtime
import time
start_time = time.time()
result = session.execute(query)
end_time = time.time()

# Print the query results
for row in result:
    print(row.title, row.author, row.borrower_name)

# Print the runtime
runtime = end_time - start_time
print("Query runtime: {:.4f} seconds".format(runtime))

# Close the Cassandra session and cluster connection
session.shutdown()
cluster.shutdown()

