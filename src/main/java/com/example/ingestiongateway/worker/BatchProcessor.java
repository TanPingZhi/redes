package com.example.ingestiongateway.worker;

import com.example.ingestiongateway.model.BatchDocument;
import com.example.ingestiongateway.model.FileTransferRequest;
import com.example.ingestiongateway.service.MinioService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class BatchProcessor {

    private final MongoTemplate mongoTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MinioService minioService;

    @Value("${app.worker.topics.alpha}")
    private String topicAlpha;

    @Value("${app.worker.topics.beta}")
    private String topicBeta;

    @Value("${app.worker.topics.ingestion:batch.ingestion.events}")
    private String ingestionTopic;

    /**
     * Recovery task: Finds batches that are stuck in READY state (e.g., missed
     * event).
     * Re-publishes them to the Kafka topic.
     */
    @Scheduled(cron = "${app.worker.cron}")
    public void recoverStuckBatches() {
        // Look for batches created more than 5 minutes ago but still READY
        // This prevents race condition with immediate event
        long threshold = System.currentTimeMillis() - (5 * 60 * 1000);

        Query query = Query.query(Criteria.where("status").is("READY").and("ingestionTimestamp").lt(threshold));
        List<BatchDocument> stuckBatches = mongoTemplate.find(query, BatchDocument.class);

        if (!stuckBatches.isEmpty()) {
            log.info("Found {} stuck READY batches. Re-publishing events.", stuckBatches.size());
            for (BatchDocument batch : stuckBatches) {
                try {
                    kafkaTemplate.send(ingestionTopic, batch);
                } catch (Exception e) {
                    log.error("Failed to re-publish batch {}", batch.getId(), e);
                }
            }
        }
    }

    @KafkaListener(topics = "${app.worker.topics.ingestion}", groupId = "ingestion-worker-group")
    public void processBatchEvent(BatchDocument batch) {
        try {
            log.info("Processing event for Batch ID: {}", batch.getId());

            // 1. Idempotency Check
            BatchDocument currentDbState = mongoTemplate.findById(batch.getId(), BatchDocument.class);
            if (currentDbState != null && "DONE".equals(currentDbState.getStatus())) {
                log.info("Batch ID: {} is already DONE. Skipping.", batch.getId());
                return;
            }

            // 2. Perform File Copy (Tmp -> Prod)
            if (batch.getTransferRequests() != null) {
                for (FileTransferRequest req : batch.getTransferRequests()) {
                    minioService.copyToProd(req);
                }
            }

            // 3. Publish Metadata to Downstream Topics
            publishToKafka(topicAlpha, batch.getKafkaMetadataAlpha());
            publishToKafka(topicBeta, batch.getKafkaMetadataBeta());

            // 4. Mark as DONE
            batch.setStatus("DONE");
            mongoTemplate.save(batch);

            log.info("Completed Batch ID: {}", batch.getId());

        } catch (Exception e) {
            log.error("Failed to process Batch Event: {}", batch.getId(), e);
        }
    }

    private void publishToKafka(String topic, List<com.example.ingestiongateway.model.FileMetadata> metadataList) {
        if (metadataList == null)
            return;

        for (com.example.ingestiongateway.model.FileMetadata meta : metadataList) {
            try {
                kafkaTemplate.send(topic, meta);
            } catch (Exception e) {
                log.error("Failed to send message to {}", topic, e);
            }
        }
    }
}
