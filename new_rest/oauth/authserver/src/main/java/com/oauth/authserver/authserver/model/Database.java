package com.oauth.authserver.authserver.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Database {
      //URL jdbc:postgresql://ec2-50-17-90-177.compute-1.amazonaws.com:5432/d2bst7bq4d5o8b?user=rmzxygxwcfirsa&password=bf66919cd252be351c10fde80a715d6b69cc55384afde16c0794a6947d2ebfa0&sslmode=require

      public static Connection getConnection() {
            //String dbUrl = System.getenv("JDBC_DATABASE_URL");
            try {
                  return DriverManager.getConnection("jdbc:postgresql://ec2-50-17-90-177.compute-1.amazonaws.com:5432/d2bst7bq4d5o8b?user=rmzxygxwcfirsa&password=bf66919cd252be351c10fde80a715d6b69cc55384afde16c0794a6947d2ebfa0&sslmode=require");

            } catch (SQLException e) {
                  e.printStackTrace();
            }
            return null;
      }
}
