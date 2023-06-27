import time
from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['localhost'])
session = cluster.connect('library_1m')
print('Connected to the Cassandra cluster.')

# Define the queries
queries = [
    """
SELECT title, author, publication_date
    FROM books
    WHERE publication_date >= '2021-01-01' AND publication_date < '2022-01-01'
    ALLOW FILTERING;
    """
]

# Execute the queries and record the execution times
for i, query in enumerate(queries):
    execution_times = []
    for j in range(30):
        start_time = time.time()
        result = session.execute(query)
        end_time = time.time()
        execution_time = end_time - start_time
        execution_times.append(execution_time)

    # Save the execution times in a text file
    filename = f'runtime1m_q2.txt'
    with open(filename, 'w') as file:
        for j, time in enumerate(execution_times):
            file.write(f'{j + 1}. {execution_time:.4f} sec\n')

    print(f'DONE! Execution times saved in {filename}.')

# Disconnect from the Cassandra cluster
session.shutdown()
print('Disconnected from the Cassandra cluster.')
cluster.shutdown()
print('Disconnected from the Cassandra cluster.')
