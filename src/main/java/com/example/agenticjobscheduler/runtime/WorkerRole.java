package com.example.agenticjobscheduler.runtime;

public enum WorkerRole {
    MAIN("jobs.main.v1", "job-main-v1"),
    RETRY("jobs.retry.v1", "job-retry-v1");

    private final String topic;
    private final String groupId;

    WorkerRole(String topic, String groupId) {
        this.topic = topic;
        this.groupId = groupId;
    }

    public String topic() {
        return topic;
    }

    public String groupId() {
        return groupId;
    }

    public static WorkerRole parse(String value) {
        return switch (value) {
            case "main" -> MAIN;
            case "retry" -> RETRY;
            default -> throw new IllegalArgumentException("WORKER_ROLE must be main or retry");
        };
    }
}
