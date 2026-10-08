package com.example.agenticjobscheduler.messaging;

/** Kafka coordinates, independent of the Kafka client API. */
public record SourceRecord(String topic, int partition, long offset) { }
