package com.example.ingestiongateway;

import com.example.ingestiongateway.model.BatchDocument;
import com.example.ingestiongateway.model.FileTransferRequest;
import com.example.ingestiongateway.service.IngestionService;
import com.example.ingestiongateway.service.MinioService;
import com.example.ingestiongateway.worker.BatchProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RecoverabilityTest {

    @Mock
    private MongoTemplate mongoTemplate;

    @Mock
    private MinioService minioService;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private IngestionService ingestionService;

    @InjectMocks
    private BatchProcessor batchProcessor;

    @BeforeEach
    void setup() {
        // Inject values
        ReflectionTestUtils.setField(ingestionService, "ingestionTopic", "batch.ingestion.events");
        ReflectionTestUtils.setField(batchProcessor, "ingestionTopic", "batch.ingestion.events");
        ReflectionTestUtils.setField(batchProcessor, "topicAlpha", "topic-alpha");
        ReflectionTestUtils.setField(batchProcessor, "topicBeta", "topic-beta");
    }

    @Test
    void testHappyPath() {
        // Arrange
        MultipartFile mockFile = mock(MultipartFile.class);
        when(mockFile.getOriginalFilename()).thenReturn("test-filev2.csv");
        when(minioService.uploadToTmp(any())).thenReturn("tmp/path/hash");

        // Act - Ingestion
        String batchId = ingestionService.processUpload(List.of(mockFile), "testUser");

        // Assert - Kafka Sent
        verify(kafkaTemplate, times(1)).send(eq("batch.ingestion.events"), any(BatchDocument.class));
        verify(mongoTemplate, times(2)).save(any(BatchDocument.class)); // 1 PENDING, 1 READY

        // Act - Processing (Simulate Listener)
        BatchDocument batchDoc = BatchDocument.builder()
                .id(batchId)
                .status("READY")
                .transferRequests(List.of(new FileTransferRequest("tmp/path/hash", "uuid", "test-filev2.csv")))
                .build();

        batchProcessor.processBatchEvent(batchDoc);

        // Assert - Processing
        verify(minioService, times(1)).copyToProd(any()); // File copied
        verify(kafkaTemplate, times(1)).send(eq("topic-alpha"), any()); // Metadata sent
        verify(mongoTemplate, times(1)).save(argThat(b -> "DONE".equals(((BatchDocument) b).getStatus())));
    }

    @Test
    void testKafkaDownRecovery() {
        // 1. Simulate Kafka Down during Ingestion
        // Arrange
        MultipartFile mockFile = mock(MultipartFile.class);
        when(mockFile.getOriginalFilename()).thenReturn("test-file.csv");
        when(minioService.uploadToTmp(any())).thenReturn("tmp/path/hash");

        // Mock Kafka Failure
        when(kafkaTemplate.send(anyString(), any(Object.class))).thenThrow(new RuntimeException("Kafka Down"));

        // Act
        ingestionService.processUpload(List.of(mockFile), "testUser");

        // Assert
        // Should not fail the request (exception caught)
        // Saved as READY in DB
        verify(mongoTemplate, times(2)).save(any(BatchDocument.class));

        // 2. Simulate Recovery Task
        // Arrange - Stuck Batch in DB
        BatchDocument stuckBatch = BatchDocument.builder().id("stuck-id").status("READY")
                .ingestionTimestamp(System.currentTimeMillis() - 600000).build();
        when(mongoTemplate.find(any(Query.class), eq(BatchDocument.class)))
                .thenReturn(Collections.singletonList(stuckBatch));

        // Reset Kafka mock to work now
        reset(kafkaTemplate);

        // Act
        batchProcessor.recoverStuckBatches();

        // Assert
        verify(kafkaTemplate, times(1)).send(eq("batch.ingestion.events"), eq(stuckBatch));
    }
}
