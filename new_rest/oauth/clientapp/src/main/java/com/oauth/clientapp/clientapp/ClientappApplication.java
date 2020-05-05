package com.oauth.clientapp.clientapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;

@SpringBootApplication
public class ClientappApplication {

	public static void main(String[] args) {
		System.setProperty("server.servlet.context-path", "/client/api");
		SpringApplication app = new SpringApplication(ClientappApplication.class);
		app.setDefaultProperties(Collections.singletonMap("server.port", "8082"));
		app.run(args);
	}

}
