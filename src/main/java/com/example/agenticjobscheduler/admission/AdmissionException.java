package com.example.agenticjobscheduler.admission;

public final class AdmissionException extends RuntimeException {
    private final int status;
    private final String code;

    public AdmissionException(int status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public static AdmissionException malformedJson() {
        return new AdmissionException(400, "MALFORMED_JSON", "Request body is not valid JSON");
    }

    public static AdmissionException validation(String message) {
        return new AdmissionException(400, "VALIDATION_ERROR", message);
    }

    public int status() {
        return status;
    }

    public String code() {
        return code;
    }
}
