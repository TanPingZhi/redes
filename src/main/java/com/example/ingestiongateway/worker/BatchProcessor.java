package com.example.ingestiongateway.worker;

import com.example.ingestiongateway.model.ESRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class BatchProcessor {

    private final ElasticsearchOperations elasticsearchOperations;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.worker.topics.alpha}")
    private String topicAlpha;

    @Value("${app.worker.topics.beta}")
    private String topicBeta;

    @Scheduled(cron = "${app.worker.cron}")
    public void processReadyBatches() {
        log.info("Starting scheduled batch processing...");

        // 1. Query for READY status across all create-replay-* indices
        Criteria criteria = new Criteria("status").is("READY");
        CriteriaQuery query = new CriteriaQuery(criteria);

        // Use IndexCoordinates to target the pattern
        SearchHits<ESRequest> hits = elasticsearchOperations.search(
                query,
                ESRequest.class,
                IndexCoordinates.of("create-replay-*"));

        if (hits.getTotalHits() == 0) {
            log.info("No READY batches found.");
            return;
        }

        List<SearchHit<ESRequest>> hitList = hits.getSearchHits();
        log.info("Found {} READY batches.", hitList.size());

        // 2. Shuffle for parallelism/work-stealing (if multiple instances)
        Collections.shuffle(hitList);

        for (SearchHit<ESRequest> hit : hitList) {
            processBatch(hit);
        }
    }

    private void processBatch(SearchHit<ESRequest> hit) {
        ESRequest request = hit.getContent();
        try {
            log.info("Processing Batch ID: {}", request.getId());

            // 3. Publish to Kafka
            publishToKafka(topicAlpha, request.getKafkaMetadataAlpha());
            publishToKafka(topicBeta, request.getKafkaMetadataBeta());

            // 4. Update Status to DONE
            request.setStatus("DONE");

            // Critical: Update the document in the SAME index it was found in.
            elasticsearchOperations.save(request, IndexCoordinates.of(hit.getIndex()));

            log.info("Completed Batch ID: {}", request.getId());

        } catch (Exception e) {
            log.error("Failed to process Batch ID: {}", request.getId(), e);
            // Optionally handle retry logic or ERROR status
        }
    }

    private void publishToKafka(String topic, List<com.example.ingestiongateway.model.FileMetadata> metadataList) {
        if (metadataList == null)
            return;

        for (com.example.ingestiongateway.model.FileMetadata meta : metadataList) {
            try {
                String message = objectMapper.writeValueAsString(meta);
                kafkaTemplate.send(topic, message);
            } catch (Exception e) {
                log.error("Failed to serialize/send message to {}", topic, e);
            }
        }
    }
}
