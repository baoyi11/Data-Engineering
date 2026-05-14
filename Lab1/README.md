## Lab Section

### **For the lab report, refer to Lab1Report.pdf in the kafka-lab folder, which contains a detailed procedure, screenshots of the execution results, and an explanation of the results for each step.**


1. **Cluster Deployment (Kraft):** I created a `docker-compose.yml` file to deploy three Kafka brokers without using ZooKeeper. It maps internal and external listeners and sets key fault-tolerance variables, such as `KAFKA_MIN_INSYNC_REPLICAS: 2`.


2. **Key-Based Routing:** By sending messages with specific keys (e.g., `user1:`), we observed that Kafka routes messages with the same key to the exact same partition, thereby ensuring message ordering.


3. **Consumer Groups and Latency:** We learned how Kafka uses offsets to track consumer progress and how to monitor “latency” to ensure consumers stay in sync with producers.


4. **Disaster Simulation (Fault Tolerance):** 

   **Test:** We intentionally crashed Broker 1 (`docker stop kafka1`).

   **Result:** We observed that Kafka’s Raft controller immediately performed a leader re-election for the affected partitions.

   **Insight:** Because the replication factor is 3, consumers still had full access to the data, demonstrating the system’s resilience.



## Exercises

### **The step-by-step solutions to the exercises can be found in the Exercises.pdf file in the kafka-lab folder.**

**Exercise 1:** Based on the failure rates of individual agents, the exact probabilities of a partition becoming read-only or completely lost were calculated. The optimal read-write quota configuration for Cassandra to guarantee strong consistency was also determined.

**Exercise 2:** Using the “balls-and-boxes” model to simulate clickstream data, we demonstrated how “heavy users” (data skew) can create hot partitions. Finally, we applied Little’s Law to calculate the consumer capacity required to process 20,000 events per second, thereby mathematically proving why simply increasing the number of consumers beyond the number of partitions does not result in any performance improvement.



## Key Reflections and Takeaways

1. **Configuration Parameters Are Variations of Mathematical Formulas:**
Variables in Docker such as `REPLICATION_FACTOR=3` and `MIN_INSYNC_REPLICAS=2` are not arbitrary numbers. They directly reflect the Bernoulli reliability model and Quorum consistency laws covered in the theoretical section.
2. **Message Queues vs. Event Streams:**
Unlike traditional queues (such as RabbitMQ), which delete messages after they are read, Kafka is an immutable, append-only log. This fundamentally changes how we design consumers, enabling replayability and supporting multiple independent consumer groups.


3. **The Importance of Keys in Distributed Systems:**
     In a single-node database, ordering is easy to guarantee. In distributed systems, however, ordering is guaranteed only within a single partition. Choosing the correct “key” for data is a critical design decision in Kafka for preventing race conditions

4. **Capacity Planning Requires Math:** 

     Little's Law and the "Parallelism Bound" ($\lambda_{max} = \min(P,C)$) prove that throwing more hardware (consumers) at a problem won't fix it if the underlying architecture (number of partitions) is the bottleneck.

5. **Embracing Failure:**

   The lab's fault-tolerance exercise showed that distributed systems aren't about preventing failures; they are about expecting them. By intentionally killing a broker, we saw firsthand how automated consensus algorithms (KRaft) seamlessly manage disaster recovery without human intervention.
