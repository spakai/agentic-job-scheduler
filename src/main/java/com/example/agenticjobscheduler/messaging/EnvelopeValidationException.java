package com.example.agenticjobscheduler.messaging;

/** Safe to report: never includes input values or parser exceptions containing payloads. */
public final class EnvelopeValidationException extends IllegalArgumentException {
    public enum Code {
        RECORD_TOO_LARGE, INVALID_UTF8, INVALID_JSON, INVALID_FIELD,
        UNKNOWN_FIELD, INVALID_KEY, UNSUPPORTED_VERSION, UNREGISTERED_JOB_TYPE,
        INVALID_RETRY_HANDOFF
    }

    private final Code code;

    public EnvelopeValidationException(Code code) {
        super(code.name());
        this.code = code;
    }

    public Code code() {
        return code;
    }
}
