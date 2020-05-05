package com.oauth.clientapp.clientapp.model;

import java.util.HashMap;
import java.util.Map;

public class Database {
      private static final String TOKEN_PATH = "http://localhost:8081/authserver/api/token";
      private static final String CLIENT_ID = "client1";
      private static final String CLIENT_SECRET = "secret1";
      private static final String REDIRECT_URL = "http://localhost:8082/client/api/callback";
      private static final String RESOURCE_URL = "http://localhost:8080/resserver/api/images";

      private static Map<String, String> usernamesPassword = new HashMap<>();
      private static Map<String, String> usernamesToken = new HashMap<>();


      public static boolean addUsernamePassword(String username, String password) {
            if(usernamesPassword.get(username) != null) {
                  return false;
            }
            usernamesPassword.put(username, password);
            return true;
      }

      public static boolean addUsernameToken(String username, String token) {
            if(usernamesToken.get(username) != null) {
                  return false;
            }
            usernamesToken.put(username, token);
            return true;
      }

      public static boolean logUsernamePassword(String username, String password) {
            if(usernamesPassword.get(username) == null) {
                  return false;
            }
            return usernamesPassword.get(username).equals(password);
      }

      public static String checkForToken(String username) {
            return usernamesToken.get(username);
      }


      public static String getTokenPath() {
            return TOKEN_PATH;
      }

      public static String getClientId() {
            return CLIENT_ID;
      }

      public static String getClientSecret() {
            return CLIENT_SECRET;
      }

      public static String getRedirectUrl() {
            return REDIRECT_URL;
      }

      public static String getResourceUrl() {
            return RESOURCE_URL;
      }
}
