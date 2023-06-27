import time
from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['localhost'])
session = cluster.connect('library250k')
print('Connected to the Cassandra cluster.')

# Define the query
query = "SELECT * FROM books;"

# Execute the query and record the execution times
execution_times = []
for i in range(30):
    start_time = time.time()
    result = session.execute(query)
    end_time = time.time()
    execution_time = end_time - start_time
    execution_times.append(execution_time)

# Save the execution times in a text file
with open('runtime250k_q1.txt', 'w') as file:
    for i, time in enumerate(execution_times):
        file.write(f'{i + 1}. {time} sec\n')

# Disconnect from the Cassandra cluster
session.shutdown()
cluster.shutdown()