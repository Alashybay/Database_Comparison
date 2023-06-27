# DB_New


## Cassandra 
To access Cassandra using Docker, you can follow these steps:

1. Install Docker: If you haven't already, install Docker on your system by following the instructions provided on the Docker website for your specific operating system.

2. Pull the Cassandra Docker image: Open a terminal or command prompt and run the following command to pull the official Cassandra Docker image:

   ```
   docker pull cassandra
   ```

3. Start a Cassandra container: Once the image is downloaded, run the following command to start a Cassandra container:

   ```
   docker run --name my-cassandra-container -p 9042:9042 -d cassandra
   ```

   This command creates a container named "my-cassandra-container" using the Cassandra image. It maps the container's Cassandra port (9042) to the same port on the host machine.

4. Access the Cassandra container: To interact with the Cassandra container, you can use the `cqlsh` command-line tool. Run the following command to start a new shell session inside the container:

   ```
   docker exec -it my-cassandra-container cqlsh
   ```

   This command uses the `docker exec` command to execute a command (`cqlsh`) inside the running container. The `-it` flags allow an interactive terminal session.

5. Interact with Cassandra: Once you are inside the container, you can execute CQL (Cassandra Query Language) commands to interact with the Cassandra database. For example, you can create a keyspace and a table:

   ```
   CREATE KEYSPACE my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
   USE my_keyspace;
   CREATE TABLE my_table (id UUID PRIMARY KEY, name TEXT);
   ```

   You can now perform various operations, such as inserting data, querying data, etc., using CQL commands.

That's it! You now have access to Cassandra using Docker. Remember to stop and remove the container when you're done by running `docker stop my-cassandra-container` and `docker rm my-cassandra-container`.