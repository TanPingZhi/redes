package com.example.ingestiongateway.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "batches")
@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchDocument {

    @Id
    private String id; // batchId

    private Integer serverId; // Server ID (0-9)

    private String status; // PENDING, READY, DONE

    // Field for automatic expiration (TTL)
    // 604800 seconds = 7 days
    @Indexed(name = "ttl_index", expireAfterSeconds = 604800)
    private Date createdAt;

    private long ingestionTimestamp;

    private UserInputRecord userInput;

    private List<FileMetadata> kafkaMetadataAlpha;

    private List<FileMetadata> kafkaMetadataBeta;

    private List<FileTransferRequest> transferRequests;
}
