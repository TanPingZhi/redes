package com.example.ingestiongateway.service;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
@RequiredArgsConstructor
@Slf4j
public class MinioService {

    private final MinioClient minioClient;

    @Value("${minio.bucket-tmp}")
    private String tmpBucketName;

    @Value("${minio.bucket-prod}")
    private String prodBucketName;

    @PostConstruct
    public void init() {
        createBucketIfNotExists(tmpBucketName);
        createBucketIfNotExists(prodBucketName);
    }

    private void createBucketIfNotExists(String bucket) {
        try {
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
                log.info("Created MinIO bucket: {}", bucket);
            } else {
                log.info("MinIO bucket already exists: {}", bucket);
            }
        } catch (Exception e) {
            log.error("Failed to initialize MinIO bucket: {}", bucket, e);
        }
    }

    /**
     * Uploads file to temporary bucket with content-based deduplication.
     * Path: yy/MM/dd/SHA256Hash
     * 
     * @return The relative path (yy/MM/dd/hash)
     */
    public String uploadToTmp(MultipartFile file) {
        try {
            // 1. Calculate Hash
            String hash = calculateSha256(file);

            // 2. Generate Date Path
            java.time.LocalDate now = java.time.LocalDate.now();
            String datePath = String.format("%02d/%02d/%02d", now.getYear() % 100, now.getMonthValue(),
                    now.getDayOfMonth());
            String objectName = datePath + "/" + hash;

            // 3. Upload (Idempotent if hash matches)
            // Check if exists to avoid re-uploading? For now just put (overwrite is fine
            // for same content)
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(tmpBucketName)
                            .object(objectName)
                            .stream(file.getInputStream(), file.getSize(), -1)
                            .contentType(file.getContentType())
                            .build());

            log.info("Uploaded to tmp: {}", objectName);
            return objectName;
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload to tmp bucket", e);
        }
    }

    public void copyToProd(com.example.ingestiongateway.model.FileTransferRequest request) {
        // Source: tmp/path
        // Dest: prod/yy/mm/dd/uuid/filename

        // Extract date from temp path or use current date?
        // User requirements: "prod-bucket/yy/mm/dd/uuid/fileid"
        // We can extract y/m/d from the tempPath or just assume it's the transfer time.
        // Let's reuse the temp path's directory structure for consistency if possible,
        // or just parse it. The temp path is "yy/mm/dd/hash".
        // Let's use the first 3 segments of tempPath.

        try {
            String[] parts = request.getTempPath().split("/");
            String datePath = parts[0] + "/" + parts[1] + "/" + parts[2];

            String destPath = datePath + "/" + request.getProdUuid() + "/" + request.getTargetFilename();

            minioClient.copyObject(
                    io.minio.CopyObjectArgs.builder()
                            .bucket(prodBucketName)
                            .object(destPath)
                            .source(
                                    io.minio.CopySource.builder()
                                            .bucket(tmpBucketName)
                                            .object(request.getTempPath())
                                            .build())
                            .build());

            log.info("Copied from {} to {}", request.getTempPath(), destPath);

        } catch (Exception e) {
            log.error("Failed to copy file to prod: {}", request, e);
            throw new RuntimeException("Failed to copy file to prod", e);
        }
    }

    private String calculateSha256(MultipartFile file) throws Exception {
        java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
        try (java.io.InputStream is = file.getInputStream()) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = is.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        StringBuilder hexString = new StringBuilder();
        for (byte b : digest.digest()) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();
    }
}
