package com.kailas;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AvroDemoApplication {

	public static void main(String[] args) {
		//System.setProperty("avro.compatibility.string", "true");

		SpringApplication.run(AvroDemoApplication.class, args);
	}

}
