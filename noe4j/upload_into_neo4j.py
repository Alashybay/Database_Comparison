from neo4j import GraphDatabase

# Connect to Neo4j
uri = "bolt://localhost:7687"
username = "library250k"
password = "Alikhan01"
driver = GraphDatabase.driver(uri, auth=(username, password))

# Upload books CSV file
with driver.session() as session:
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///books.csv' AS row
        CREATE (:Book {
            book_id: toInteger(row.book_id),
            title: row.title,
            author: row.author,
            publication_date: date(row.publication_date)
        })
    """)

# Upload borrowers CSV file
with driver.session() as session:
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///borrowers.csv' AS row
        CREATE (:Borrower {
            borrower_id: toInteger(row.borrower_id),
            name: row.name,
            email: row.email
        })
    """)

# Upload borrowing_history CSV file
with driver.session() as session:
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///borrowing_history.csv' AS row
        MATCH (book:Book {book_id: toInteger(row.book_id)})
        MATCH (borrower:Borrower {borrower_id: toInteger(row.borrower_id)})
        CREATE (book)-[:BORROWED_BY {
            borrowing_id: toInteger(row.borrowing_id),
            borrow_date: date(row.borrow_date),
            return_date: date(row.return_date)
        }]->(borrower)
    """)

# Close the Neo4j driver
driver.close()
