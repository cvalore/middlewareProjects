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
            dataSource.setUrl("jdbc:postgresql://ec2-50-17-90-177.compute-1.amazonaws.com:5432/d2bst7bq4d5o8b?user=rmzxygxwcfirsa&password=bf66919cd252be351c10fde80a715d6b69cc55384afde16c0794a6947d2ebfa0&sslmode=require");
            return dataSource;
      }

      @Bean
      public PasswordEncoder passwordEncoder() {
            return new BCryptPasswordEncoder();
      }
}
