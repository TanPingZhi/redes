package com.example.ingestiongateway.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class ServerIdentityService {

    private final MongoTemplate mongoTemplate;

    private Integer myServerId;
    private final String leaseHolderId = UUID.randomUUID().toString();
    private static final String COLLECTION_NAME = "server_registry";
    private static final long HEARTBEAT_TIMEOUT_MS = 30000; // 30 seconds timeout

    @PostConstruct
    public void acquireServerIdentity() {
        log.info("Attempting to acquire server ID...");

        for (int i = 0; i < 10; i++) {
            // Try to claim ID 'i' if it's free OR expired
            Query query = new Query(Criteria.where("_id").is(i)
                    .orOperator(
                            Criteria.where("leaseHolder").exists(false),
                            Criteria.where("lastHeartbeat").lt(System.currentTimeMillis() - HEARTBEAT_TIMEOUT_MS)));

            Update update = new Update()
                    .set("leaseHolder", leaseHolderId)
                    .set("lastHeartbeat", System.currentTimeMillis());

            try {
                // Try to claim the ID. If it doesn't exist, upsert will create it.
                // If it exists but is expired/free, update will claim it.
                Object result = mongoTemplate.findAndModify(
                        query,
                        update,
                        FindAndModifyOptions.options().upsert(true).returnNew(true),
                        Object.class,
                        COLLECTION_NAME);

                if (result != null) {
                    this.myServerId = i;
                    log.info("Successfully acquired Server ID: {}", myServerId);
                    return;
                }
            } catch (Exception e) {
                // Ignore duplicate key or other errors, try next ID
            }
        }

        if (this.myServerId == null) {
            throw new RuntimeException("Failed to acquire a Server ID within range 0-9. All 10 slots are busy.");
        }
    }

    @Scheduled(fixedRate = 10000)
    public void heartbeat() {
        if (myServerId != null) {
            Query query = new Query(Criteria.where("_id").is(myServerId).and("leaseHolder").is(leaseHolderId));
            Update update = new Update().set("lastHeartbeat", System.currentTimeMillis());
            mongoTemplate.updateFirst(query, update, COLLECTION_NAME);
        }
    }

    @PreDestroy
    public void releaseIdentity() {
        if (myServerId != null) {
            Query query = new Query(Criteria.where("_id").is(myServerId).and("leaseHolder").is(leaseHolderId));
            mongoTemplate.remove(query, COLLECTION_NAME);
            log.info("Released Server ID: {}", myServerId);
        }
    }

    public Integer getServerId() {
        return myServerId;
    }
}
