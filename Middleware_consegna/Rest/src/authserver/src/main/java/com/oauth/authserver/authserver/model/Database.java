package com.oauth.authserver.authserver.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Database {
      //URL jdbc:postgresql://ec2-54-211-210-149.compute-1.amazonaws.com:5432/da6fbrmsn5gpgv?user=thewxbznejcmpu&password=c20ac4cdd2142245dfe01725587b4c68b9021c3526c0f6532aeed684fd37ea68&sslmode=require

      public static final int EXPIRES_AUTH_CODE = 8; //minutes
      public static final int EXPIRES_TOKEN = 45; //minutes
      public static final String ME = "https://carmelo-polimi-auth-server.herokuapp.com/authserver/api";
      public static final String TEST_KEY = "test_key";

      public static Connection getConnection() {
            //String dbUrl = System.getenv("JDBC_DATABASE_URL");
            try {
                  return DriverManager.getConnection("jdbc:postgresql://ec2-54-211-210-149.compute-1.amazonaws.com:5432/da6fbrmsn5gpgv?user=thewxbznejcmpu&password=c20ac4cdd2142245dfe01725587b4c68b9021c3526c0f6532aeed684fd37ea68&sslmode=require");

            } catch (SQLException e) {
                  e.printStackTrace();
            }
            return null;
      }


}
