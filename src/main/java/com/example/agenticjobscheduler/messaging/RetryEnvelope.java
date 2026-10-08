package com.example.agenticjobscheduler.messaging;

public record RetryEnvelope(int schemaVersion, JobEnvelope job, int attempt,
                            String handoffId, SourceRecord originalSource,
                            SourceRecord failedSource, String errorCode) { }
