package com.oauth.authserver.authserver.model;

import java.sql.Timestamp;

public class Code {
      private String authCode;
      private String clientId;
      private String redirectUrl;
      private Timestamp expiresIn;

      public Code(String authCode, String clientId, String redirectUrl, Timestamp expiresIn) {
            this.authCode = authCode;
            this.clientId = clientId;
            this.redirectUrl = redirectUrl;
            this.expiresIn = expiresIn;
      }

      public String getAuthCode() {
            return authCode;
      }

      public String getClientId() {
            return clientId;
      }

      public String getRedirectUrl() {
            return redirectUrl;
      }

      public Timestamp getExpiresIn() {
            return expiresIn;
      }
}
