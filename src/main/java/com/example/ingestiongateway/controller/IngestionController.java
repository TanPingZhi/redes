package com.example.ingestiongateway.controller;

import com.example.ingestiongateway.service.IngestionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api/batches")
@RequiredArgsConstructor
public class IngestionController {

    private final IngestionService ingestionService;

    @PostMapping("/upload")
    public ResponseEntity<String> uploadBatch(
            @RequestParam("files") List<MultipartFile> files,
            @RequestParam("userName") String userName) throws IOException {

        String batchId = ingestionService.processUpload(files, userName);
        return ResponseEntity.ok(batchId);
    }
}
