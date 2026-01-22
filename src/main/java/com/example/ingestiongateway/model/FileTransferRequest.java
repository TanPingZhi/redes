package com.example.ingestiongateway.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileTransferRequest {

    /**
     * Relative path in the temp bucket.
     * Format: yy/mm/dd/hash
     */
    private String tempPath;

    /**
     * UUID generated for the production path.
     */
    private String prodUuid;

    /**
     * Original filename or target identifier for the final file.
     */
    private String targetFilename;
}
