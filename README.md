

####
Kafka Stateful Message Processor
This project implements a streaming system using Apache Kafka, which categorizes messages into various states and ensures their reliable processing and storage.

System Overview
Kafka Producer: Produces 1000 messages as a batch into a Kafka topic, with each message being in one of three states: failed, completed, or in progress.
Kafka Consumer: Reads messages from the Kafka topic, parses the state, and stores them into a persistent storage system.
Persistent Store: A database that retains messages with their respective states.
Test Suite: Validates that no messages are dropped and that each message is correctly parsed into its respective state.
Getting Started
Prerequisites
go 1.7+
