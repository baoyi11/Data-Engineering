## Part 1: Infrastructure and Kafka topic

The objective is to deploy a 3-broker Apache Kafka cluster using Docker Compose (KRaft mode) to ensure a distributed architecture with fault tolerance. We must then create a highly available topic named `sensor-events` with 3 partitions and a replication factor of 3. Finally, we demonstrate fault tolerance by terminating a broker.  

Reuse (or recreate) the docker-compose.yml from Session 1 and start the cluster.

```powershell
docker compose up -d
docker ps
```

![image-20260519082130972](D:/EFREI/Data_Engineering/LAB/Exam/Exam.assets/image-20260519082130972.png)

```
# Create topic
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --topic sensor-events --partitions 3 --replication-factor 3 --config min.insync.replicas=2
# Describe topic
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
```

![image-20260519082411580](D:/EFREI/Data_Engineering/LAB/Exam/Exam.assets/image-20260519082411580.png)

Run a fault tolerance test

```
docker stop kafka2
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
```

![image-20260519082618630](D:/EFREI/Data_Engineering/LAB/Exam/Exam.assets/image-20260519082618630.png)

The initial output confirms that the topic has been successfully created. After stopping `kafka2`, Partition 3 (which was previously led by `kafka2`) seamlessly transferred leadership to `kafka3`. The number of ISRs (in-sync replicas) has been reduced to 1 and 3, but since `min.insync.replicas=2`, the topic remains operational and fault-tolerant.