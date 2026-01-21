package com.example.ingestiongateway;

import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class VerifyIngestion {

    private static final String API_URL = "http://localhost:8080/api/batches/upload";

    public static void main(String[] args) throws IOException, InterruptedException {
        RestTemplate restTemplate = new RestTemplateBuilder().build();

        // 1. Create dummy files
        File file1 = createDummyFile("test-file-1.txt", "Content of file 1");
        File file2 = createDummyFile("test-file-2.txt", "Content of file 2");

        // 2. Prepare Multipart Request
        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("files", new FileSystemResource(file1));
        body.add("files", new FileSystemResource(file2));
        body.add("userName", "testUser");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);

        // 3. Send Request
        System.out.println("Sending upload request to " + API_URL);
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(API_URL, requestEntity, String.class);
            String batchId = response.getBody();
            System.out.println("Success! Batch ID: " + batchId);
            System.out.println(
                    "Verify in Elasticsearch (http://localhost:9200/create-replay-*/_search?q=_id:" + batchId + ")");
            System.out.println("Verify in MinIO Console (http://localhost:9001 -> prod-bucket/data/" + batchId + ")");
        } catch (Exception e) {
            System.err.println("Request failed: " + e.getMessage());
            e.printStackTrace();
        }

        // Cleanup
        file1.delete();
        file2.delete();
    }

    private static File createDummyFile(String name, String content) throws IOException {
        File file = new File(name);
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
        }
        return file;
    }
}
