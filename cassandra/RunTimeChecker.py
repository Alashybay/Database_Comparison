from cassandra.cluster import Cluster
import time

# Connect to Cassandra cluster
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('library_250k')
print('Connected to the Cassandra cluster.')

# Define the queries
queries = [
    """
    """
]

# Run each query 30 times and record execution times
execution_times = []
for i, query in enumerate(queries):
    print(f"Running query {i+1}")
    times = []
    for j in range(30):
        start_time = time.time()
        session.execute(query)
        end_time = time.time()
        execution_time = end_time - start_time
        times.append(execution_time)
    
    execution_times.append(times)
    print(f"Query {i+1} executed {len(times)} times.")

# Save execution times to a file
with open("runtime250k_q3.txt", "w") as file:
    for i, times in enumerate(execution_times):
        file.write(f"{i+1}. {', '.join([f'{t:.4f} sec' for t in times])}\n")

print("Execution times saved in runtime250k_q3.txt.")

# Close the connection
session.shutdown()
cluster.shutdown()
print('Disconnected from the Cassandra cluster.')
