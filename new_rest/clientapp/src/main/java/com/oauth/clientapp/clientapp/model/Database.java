package com.oauth.clientapp.clientapp.model;

import java.util.HashMap;
import java.util.Map;

public class Database {
      public static final String CLIENT_ID = "client1";
      public static final String CLIENT_SECRET = "secret1";
      //public static final String TOKEN_PATH = "http://localhost:8081/authserver/api/token";
      public static final String TOKEN_PATH = "https://carmelo-polimi-auth-server.herokuapp.com/authserver/api/token";
      //public static final String AUTH_URL = "http://localhost:8081/authserver/api/auth";
      public static final String AUTH_URL = "https://carmelo-polimi-auth-server.herokuapp.com/authserver/api/auth";
      //public static final String REDIRECT_URL = "http://localhost:8082/client/api/callback";
      public static final String REDIRECT_URL = "https://carmelo-polimi-client-ex.herokuapp.com/client/api/callback";
      //public static final String RESOURCE_URL = "http://localhost:8080/resserver/api/images";
      public static final String RESOURCE_URL = "https://carmelo-polimi-res-server.herokuapp.com/resserver/api/images";


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
                  usernamesToken.remove(username);
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

      public static void deleteToken(String username) {
            usernamesToken.remove(username);
      }
}
