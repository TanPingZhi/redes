package com.example.ingestiongateway.model;

import java.util.List;

public record FileMetadata(
        String filename,
        String batchId,
        long size,
        List<InnerRecord> innerRecords) {
}
