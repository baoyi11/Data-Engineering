# Lab2 Report and Exercises2



### **Open the LAB2 folder, which contains lab2_report.pdf, Exercises.pdf, consumer.py, producer.py, and docker-compose.yml. It includes all the step-by-step instructions, screenshots of the results, and reflections.**



## Lab2 Report

### Step 0: Environment Setup

- Starts the Kafka cluster environment using Docker Compose.  
- Configures the virtual environment and installs the `kafka-python-ng` library.  
- Creates a topic named `sensor-events` configured with 3 partitions and a replication factor of 3.  

### Step 1 & 2: Write the Producer & Consumer

- **Producer**: Configured for high durability with settings like `acks='all'` and `retries=5`. Implements a loop to simulate sending sensor readings (temperature, humidity, pressure), using the sensor type as the message key to guarantee intra-partition chronological ordering.  
- **Consumer**: Configured with `enable_auto_commit=False` for manual offset control. Implements a processing loop that manually commits offsets only after the message is fully processed (e.g., triggering a high-temperature alert), successfully achieving an "at-least-once" delivery pattern.  

### Step 3 & 4: Pipeline Execution & Consumer Group Scaling

- Runs an end-to-end test, verifying that specific sensor readings (like temperature) strictly land on identical partitions.  
- Starts a second consumer instance to observe how Kafka scales consumption by distributing partitions among multiple consumers in the same group.  
- Verifies partition assignments programmatically and observes Kafka trigger a rebalance, redistributing tasks when a consumer leaves the group.  

### Step 5 & 6: Fault Injection & Monitoring

- **Crash and Replay**: Simulates an application failure by suddenly terminating the consumer before manual commits occur, observing the safe replay of uncommitted messages upon restart.  
- **UI Monitoring**: Uses Kafka UI (accessed at `http://localhost:8080`) to visually monitor the health status and lag of the `sensor-analytics` consumer group.  



## Exercises Exercises 2

### Exercise 1: Producer Reliability and Retries

- **Acknowledgements**: Calculates the loss probability and expected lost records under different acknowledgment configurations (e.g., `acks=1` and `acks=all` with different `min.insync.replicas` settings).  
- **Retries**: Calculates the expected number of attempts to deliver a record and the end-to-end latency based on a specific transient error probability.  
- Explores the overall residual loss probability after configuring a specific number of retries.  

### Exercise 2: Consumer Groups, Lag, and Delivery Semantics

- **Capacity and Lag**: Analyzes the average number of partitions handled by consumers, calculates aggregate consumer throughput, and evaluates accumulated lag during traffic spikes and the subsequent catch-up time.  
- **Scaling and Rebalancing**: Discusses the maximum useful number of consumers for a specific partition count and the minimum partitions needed to support higher producer rates.  
- **Delivery Semantics**: Calculates expected processing counts under at-least-once delivery with a given crash probability.  