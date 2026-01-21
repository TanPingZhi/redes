package com.example.ingestiongateway.config;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import co.elastic.clients.elasticsearch.ElasticsearchClient;

import java.io.IOException;
import java.io.StringReader;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class ElasticsearchConfig {

  private final ElasticsearchClient esClient;

  private static final String POLICY_NAME = "create-replay-policy";
  private static final String TEMPLATE_NAME = "create-replay-template";
  private static final String INDEX_PATTERN = "create-replay-*";

  @PostConstruct
  public void setupIlmAndTemplate() {
    // Retry with exponential backoff because ES may not be ready when app starts
    int maxRetries = 10;
    int retryDelayMs = 2000;

    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        log.info("Attempting to configure Elasticsearch ILM/Template (attempt {}/{})", attempt, maxRetries);
        setupLifecyclePolicy();
        setupIndexTemplate();
        log.info("Elasticsearch ILM policy and template configured successfully.");
        return; // Success, exit the retry loop
      } catch (Exception e) {
        log.warn("Failed to configure Elasticsearch ILM/Template (attempt {}/{}): {}",
            attempt, maxRetries, e.getMessage());
        if (attempt < maxRetries) {
          try {
            Thread.sleep(retryDelayMs);
            retryDelayMs = Math.min(retryDelayMs * 2, 30000); // Exponential backoff, max 30s
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        } else {
          log.error("Failed to configure Elasticsearch ILM/Template after {} attempts", maxRetries, e);
        }
      }
    }
  }

  private void setupLifecyclePolicy() throws IOException {
    // Define ILM Policy: Hot -> Delete after 30 days (example)
    // Adjust retention as needed per requirements.
    // Note: The low level client might be needed for complex raw JSON,
    // but checking if we can do this via the Java API client simply.

    // For simplicity in this demo, we might skip the detailed complex ILM JSON
    // construction
    // if the Java API doesn't support it easily, but let's try a basic one.
    // Actually, simplest is to log that we are skipping strictly enforcing it via
    // code
    // if the library usage is verbose, but let's try to be robust.

    // Retain for 7 days
    String policyJson = """
        {
          "policy": {
            "phases": {
              "hot": {
                "min_age": "0ms",
                "actions": {}
              },
              "delete": {
                "min_age": "7d",
                "actions": {
                  "delete": {}
                }
              }
            }
          }
        }
        """;

    esClient.ilm().putLifecycle(r -> r
        .name(POLICY_NAME)
        .withJson(new StringReader(policyJson)));
  }

  private void setupIndexTemplate() throws IOException {
    // Template to apply the policy to create-replay-*
    String templateJson = """
        {
          "index_patterns": ["create-replay-*"],
          "template": {
            "settings": {
              "index.lifecycle.name": "create-replay-policy",
               "number_of_shards": 1,
               "number_of_replicas": 0
            }
          }
        }
        """;

    esClient.indices().putIndexTemplate(r -> r
        .name(TEMPLATE_NAME)
        .withJson(new StringReader(templateJson)));
  }
}
