package com.example.ingestiongateway.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import static org.junit.jupiter.api.Assertions.*;

public class ServerIdentityIntegrationTest {

    private MongoClient mongoClient;
    private MongoTemplate mongoTemplate;
    private ServerIdentityService service;

    // We assume the test runs in a container. To access the host's localhost:27017
    // (where Mongo is mapped),
    // we use 'host.docker.internal' (Docker Desktop for Windows feature).
    // Note: requires --add-host host.docker.internal:host-gateway in docker run.
    private static final String MONGO_URI = "mongodb://host.docker.internal:27017/integration_test_db";

    @BeforeEach
    void setUp() {
        mongoClient = MongoClients.create(MONGO_URI);
        mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(mongoClient, "integration_test_db"));
        service = new ServerIdentityService(mongoTemplate);

        // Clean upstream
        mongoTemplate.dropCollection("server_registry");
    }

    @AfterEach
    void tearDown() {
        // service.releaseIdentity(); // optional, but test logic calls it
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    void testAcquireAndReleaseRealMongo() {
        System.out.println("Connecting to Real MongoDB at " + MONGO_URI);

        // 1. Acquire
        service.acquireServerIdentity();
        Integer id = service.getServerId();
        assertNotNull(id, "Server ID should be assigned");
        assertTrue(id >= 0 && id <= 9, "Server ID must be 0-9");
        System.out.println("Acquired ID: " + id);

        // Verify in DB
        MongoDatabase db = mongoClient.getDatabase("integration_test_db");
        MongoCollection<Document> col = db.getCollection("server_registry");
        Document doc = col.find(new Document("_id", id)).first();
        assertNotNull(doc, "Document should exist in DB");
        assertNotNull(doc.getString("leaseHolder"), "leaseHolder should be set");

        // 2. Release
        service.releaseIdentity();

        // Verify gone
        doc = col.find(new Document("_id", id)).first();
        assertNull(doc, "Document should be removed after release");
        System.out.println("Released ID: " + id);
    }
}
