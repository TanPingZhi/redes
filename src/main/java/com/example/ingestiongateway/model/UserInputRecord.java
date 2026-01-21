package com.example.ingestiongateway.model;

public record UserInputRecord(
        String userName,
        String requestType, // Example of another field
        InnerRecord details // Nested record example
) {
}
