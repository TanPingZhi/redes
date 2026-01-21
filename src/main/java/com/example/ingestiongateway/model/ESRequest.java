package com.example.ingestiongateway.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.List;

@Data
@Builder
@Document(indexName = "create-replay-#{T(java.time.LocalDate).now().toString()}", createIndex = false)
// createIndex = false because we rely on Elasticsearch Index Templates (set up
// in ElasticsearchConfig)
// to create indices with the correct ILM policies and settings automatically.
@JsonIgnoreProperties(ignoreUnknown = true)
public class ESRequest {

    @Id
    private String id; // batchId

    @Field(type = FieldType.Keyword)
    private String status; // PENDING, READY, DONE

    @Field(type = FieldType.Long)
    private long ingestionTimestamp;

    @Field(type = FieldType.Object)
    private UserInputRecord userInput;

    @Field(type = FieldType.Object)
    private List<FileMetadata> kafkaMetadataAlpha;

    @Field(type = FieldType.Object)
    private List<FileMetadata> kafkaMetadataBeta;
}
