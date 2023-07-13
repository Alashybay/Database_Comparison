import redis

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Step 1: Perform the $lookup equivalent by fetching all book IDs
books = r.keys("book:*")
for book_key in books:
    book = r.hgetall(book_key)

    if b'book_id' in book:
        book_id = book[b'book_id'].decode('utf-8')
        print(book_id)
        # Fetch borrowing history data for the book
        borrowing_key = f"borrowing_history:{book_id}"
        borrowing_history = r.hgetall(borrowing_key)
        print(borrowing_history)
        # Filter by author and borrowing year
        if (
            book.get(b'author', b'').decode('utf-8') == 'Rebecca Patterson' and
            borrowing_history.get(b'borrow_year', b'').decode('utf-8') == '2022'
        ):
            result = {
                '_id': book_key.split(':')[1],
                'title': book.get(b'title', b'').decode('utf-8'),
                'author': book.get(b'author', b'').decode('utf-8'),
                'borrow_date': borrowing_history.get(b'borrow_date', b'').decode('utf-8')
            }
            print(result)
