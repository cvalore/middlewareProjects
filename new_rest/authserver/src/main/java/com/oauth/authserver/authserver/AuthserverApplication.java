package com.oauth.authserver.authserver;

import com.oauth.authserver.authserver.model.Database;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;

@SpringBootApplication
public class AuthserverApplication {

	public static void main(String[] args) {
		//fake init
		//Database.fakeInit();

		System.setProperty("server.servlet.context-path", "/authserver/api");
		SpringApplication app = new SpringApplication(AuthserverApplication.class);
		app.setDefaultProperties(Collections.singletonMap("server.port", "8081"));
		app.run(args);
	}

}
