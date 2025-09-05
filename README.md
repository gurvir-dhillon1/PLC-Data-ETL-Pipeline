# PLC Data ETL Pipeline

A prototype ETL pipeline for collecting, streaming, and consuming simulated PLC sensor data using **Kafka**.

## Overview
1. **Producers** generate random sensor readings for multiple machines and send them to Kafka.  
2. **Kafka** acts as a high-throughput message broker.  
3. **Consumers** read messages from Kafka for processing, storage, or analytics (currently just prints out the thread + message to console).  

This setup is fully containerized and can be run locally for development and experimentation.
