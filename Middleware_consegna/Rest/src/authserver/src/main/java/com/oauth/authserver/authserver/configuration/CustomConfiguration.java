package com.oauth.authserver.authserver.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@Configuration
@EnableWebMvc
@ComponentScan(basePackages = "com.oauth.authserver.authserver")
public class CustomConfiguration {
      @Bean
      public DriverManagerDataSource getDatasource(){
            DriverManagerDataSource dataSource = new DriverManagerDataSource();
            dataSource.setDriverClassName("org.postgresql.Driver");
            dataSource.setUrl("jdbc:postgresql://ec2-54-211-210-149.compute-1.amazonaws.com:5432/da6fbrmsn5gpgv?user=thewxbznejcmpu&password=c20ac4cdd2142245dfe01725587b4c68b9021c3526c0f6532aeed684fd37ea68&sslmode=require");
            return dataSource;
      }

      @Bean
      public PasswordEncoder passwordEncoder() {
            return new BCryptPasswordEncoder();
      }
}
