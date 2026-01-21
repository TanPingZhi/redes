package com.example.ingestiongateway.service;

import com.example.ingestiongateway.model.ESRequest;
import com.example.ingestiongateway.model.FileMetadata;
import com.example.ingestiongateway.model.InnerRecord;
import com.example.ingestiongateway.model.UserInputRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class IngestionService {

    private final ElasticsearchOperations elasticsearchOperations;
    private final MinioService minioService;

    public String processUpload(List<MultipartFile> files, String userName) {
        String batchId = UUID.randomUUID().toString();
        long timestamp = Instant.now().toEpochMilli();

        // 1. Prepare Metadata Lists (Simulating the requirement: "2 lists of metadata
        // for each kafka queue")
        List<FileMetadata> metadataAlpha = new ArrayList<>();
        List<FileMetadata> metadataBeta = new ArrayList<>();

        for (MultipartFile file : files) {
            List<InnerRecord> innerRecords = List.of(
                    new InnerRecord("type", file.getContentType()),
                    new InnerRecord("status", "received"));
            FileMetadata meta = new FileMetadata(
                    file.getOriginalFilename(),
                    batchId,
                    file.getSize(),
                    innerRecords);

            // Just duplicating for demo purposes
            metadataAlpha.add(meta); // Copy for Alpha
            metadataBeta.add(meta); // Copy for Beta
        }

        // 2. Index PENDING Document
        // constructing a record with nested data to demonstrate capability
        UserInputRecord userInput = new UserInputRecord(
                userName,
                "upload",
                new InnerRecord("source", "web-api"));

        ESRequest request = ESRequest.builder()
                .id(batchId)
                .status("PENDING")
                .ingestionTimestamp(timestamp)
                .userInput(userInput)
                .kafkaMetadataAlpha(metadataAlpha)
                .kafkaMetadataBeta(metadataBeta)
                .build();

        elasticsearchOperations.save(request);
        log.info("Created PENDING request for Batch ID: {}", batchId);

        // 3. Upload to MinIO (Async or Sync? Sync is safer for "all files done then
        // READY")
        // If files are huge, we might want to do this async, but the requirement says
        // "after all files are done, then we change status to ready".
        // Parallel stream for speed if multiple files.
        try {
            files.parallelStream().forEach(file -> minioService.uploadFile(batchId, file));
        } catch (Exception e) {
            // Handle cleanup or error state if needed
            log.error("Upload failed for Batch ID: {}", batchId, e);
            throw e;
        }

        // 4. Update Status to READY
        request.setStatus("READY");
        elasticsearchOperations.save(request);
        log.info("Batch ID: {} is now READY", batchId);

        return batchId;
    }
}
