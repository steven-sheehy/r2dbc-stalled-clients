package com.example.r2dbc.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.transaction.reactive.ReactiveTransactionAutoConfiguration;

@SpringBootApplication(exclude = ReactiveTransactionAutoConfiguration.class)
public class R2dbcExampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(R2dbcExampleApplication.class, args);
	}
}
