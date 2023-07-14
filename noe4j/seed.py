from neo4j import GraphDatabase

# Credentials
uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"
database = "lib500"

# Connection to Neo4j
driver = GraphDatabase.driver(uri, auth=(user, password), database=database)

# Session
session = driver.session()

# Load and insert data from books.csv
def seedBooks():
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///books.csv' AS line
        CREATE (:Book {
        book_id: toInteger(line.book_id),
        title: line.title,
        author: line.author,
        publication_date: line.publication_date
        })
        """)

# Load and insert data from borrowers.csv
def seedBorrowers():
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///borrowers.csv' AS line
        CREATE (:Borrower {
        borrower_id: toInteger(line.borrower_id),
        name: line.name,
        email: line.email
        })
        """)

# Load and insert data from borrowing_history.csv
def seedBorrowingHistory():
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///borrowing_history.csv' AS line
        CREATE (:BorrowingHistory {
        borrowing_id: toInteger(line.borrowing_id),
        book_id: toInteger(line.book_id),
        borrower_id: toInteger(line.borrower_id),
        borrow_date: line.borrow_date,
        return_date: line.return_date
        })
        """)

# Call the seeding functions
seedBooks()
seedBorrowers()
seedBorrowingHistory()

# Close session and driver
session.close()
driver.close()
