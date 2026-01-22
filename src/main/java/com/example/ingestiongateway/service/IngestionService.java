package com.example.ingestiongateway.service;

import com.example.ingestiongateway.model.BatchDocument;
import com.example.ingestiongateway.model.FileMetadata;
import com.example.ingestiongateway.model.InnerRecord;
import com.example.ingestiongateway.model.UserInputRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import org.springframework.kafka.core.KafkaTemplate;
import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class IngestionService {

    private final MongoTemplate mongoTemplate;
    private final MinioService minioService;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @org.springframework.beans.factory.annotation.Value("${app.worker.topics.ingestion:batch.ingestion.events}")
    private String ingestionTopic;

    public String processUpload(List<MultipartFile> files, String userName) {
        String batchId = UUID.randomUUID().toString();
        long timestamp = Instant.now().toEpochMilli();

        // 1. Create and Save PENDING Document
        UserInputRecord userInput = new UserInputRecord(
                userName,
                "upload",
                new InnerRecord("source", "web-api"));

        BatchDocument document = BatchDocument.builder()
                .id(batchId)
                .status("PENDING")
                .ingestionTimestamp(timestamp)
                .createdAt(new Date())
                .userInput(userInput)
                .build();

        mongoTemplate.save(document);
        log.info("Saved PENDING batch: {}", batchId);

        // DELAY FOR DEMO PURPOSES
        try {
            log.info("Sleeping for 5 seconds to show PENDING state...");
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 2. Prepare Metadata Lists & Transfer Requests
        List<FileMetadata> metadataAlpha = new ArrayList<>();
        List<FileMetadata> metadataBeta = new ArrayList<>();
        List<com.example.ingestiongateway.model.FileTransferRequest> transferRequests = new ArrayList<>();

        try {
            for (MultipartFile file : files) {
                // Upload to Tmp Bucket
                String tmpPath = minioService.uploadToTmp(file);

                // Generate Prod UUID and Request
                String prodUuid = UUID.randomUUID().toString();
                String originalFilename = file.getOriginalFilename();

                com.example.ingestiongateway.model.FileTransferRequest request = com.example.ingestiongateway.model.FileTransferRequest
                        .builder()
                        .tempPath(tmpPath)
                        .prodUuid(prodUuid)
                        .targetFilename(originalFilename)
                        .build();
                transferRequests.add(request);

                // Create Metadata
                List<InnerRecord> innerRecords = List.of(
                        new InnerRecord("type", file.getContentType()),
                        new InnerRecord("status", "received"));
                FileMetadata meta = new FileMetadata(
                        originalFilename,
                        batchId,
                        file.getSize(),
                        innerRecords);

                metadataAlpha.add(meta);
                metadataBeta.add(meta);
            }

            // 3. Update and Save READY Document
            document.setStatus("READY");
            document.setKafkaMetadataAlpha(metadataAlpha);
            document.setKafkaMetadataBeta(metadataBeta);
            document.setTransferRequests(transferRequests);

            mongoTemplate.save(document);
            log.info("Saved READY batch: {}", batchId);

            // 4. Fire Event to Kafka
            try {
                kafkaTemplate.send(ingestionTopic, document);
                log.info("Published ingestion event for Batch ID: {}", batchId);
            } catch (Exception e) {
                log.error("Failed to publish ingestion event for Batch ID: {} - Worker will recover.", batchId, e);
            }

        } catch (Exception e) {
            log.error("Upload/Ingestion failed for Batch ID: {}", batchId, e);
            // Optional: Update status to FAILED here if desired, but throw for now
            throw new RuntimeException("Ingestion failed", e);
        }

        return batchId;
    }
}
