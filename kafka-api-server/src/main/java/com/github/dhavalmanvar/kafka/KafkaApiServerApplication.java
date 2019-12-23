package com.github.dhavalmanvar.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.github.dhavalmanvar.kafka.*")
public class KafkaApiServerApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaApiServerApplication.class, args);
	}

}
