package com.example.ingestiongateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class IngestionGatewayApplication {

	public static void main(String[] args) {
		SpringApplication.run(IngestionGatewayApplication.class, args);
	}

}
