package com.oauth.authserver.authserver.model;

public class Token {
      private String token;
      private String code;

      public Token(String token, String code) {
            this.token = token;
            this.code = code;
      }

      public String getToken() {
            return token;
      }

      public String getCode() {
            return code;
      }
}
