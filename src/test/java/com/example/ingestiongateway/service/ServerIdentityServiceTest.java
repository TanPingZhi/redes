package com.example.ingestiongateway.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ServerIdentityServiceTest {

    @Mock
    private MongoTemplate mongoTemplate;

    @InjectMocks
    private ServerIdentityService serverIdentityService;

    @BeforeEach
    void setUp() {
    }

    @Test
    void acquireServerIdentity_Success() {
        // Arrange
        // Simulate findAndModify returning a non-null object (success)
        when(mongoTemplate.findAndModify(any(Query.class), any(Update.class), any(FindAndModifyOptions.class),
                eq(Object.class), anyString()))
                .thenReturn(new Object());

        // Act
        serverIdentityService.acquireServerIdentity();

        // Assert
        assertEquals(0, serverIdentityService.getServerId());
    }

    @Test
    void acquireServerIdentity_FailAll() {
        // Arrange
        // Simulate findAndModify returning null for all 10 tries
        when(mongoTemplate.findAndModify(any(Query.class), any(Update.class), any(FindAndModifyOptions.class),
                eq(Object.class), anyString()))
                .thenReturn(null);

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            serverIdentityService.acquireServerIdentity();
        });

        assertEquals("Failed to acquire a Server ID within range 0-9. All 10 slots are busy.", exception.getMessage());
        verify(mongoTemplate, times(10)).findAndModify(any(Query.class), any(Update.class),
                any(FindAndModifyOptions.class), eq(Object.class), anyString());
    }

    @Test
    void releaseIdentity_Success() {
        // Arrange
        // We need to set the internal state first.
        // Since myServerId is private and we can't easily set it without reflection or
        // calling acquire,
        // we'll call acquire first with success mock.
        when(mongoTemplate.findAndModify(any(Query.class), any(Update.class), any(FindAndModifyOptions.class),
                eq(Object.class), anyString()))
                .thenReturn(new Object());
        serverIdentityService.acquireServerIdentity();

        // Act
        serverIdentityService.releaseIdentity();

        // Assert
        verify(mongoTemplate).remove(any(Query.class), eq("server_registry"));
    }
}
