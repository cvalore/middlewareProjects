package com.oauth.authserver.authserver.model;

public class ClientDetails {
      private String clientId;
      private String redirectUrl;
      private String clientSecret;
      private String grantType;

      public ClientDetails() {
      }

      public ClientDetails(String clientId, String redirectUrl) {
            this.clientId = clientId;
            this.redirectUrl = redirectUrl;
      }

      public ClientDetails(String clientId, String redirectUrl, String clientSecret, String grantType) {
            this.clientId = clientId;
            this.redirectUrl = redirectUrl;
            this.clientSecret = clientSecret;
            this.grantType = grantType;
      }

      public String getClientId() {
            return clientId;
      }

      public String getRedirectUrl() {
            return redirectUrl;
      }

      public String getClientSecret() {
            return clientSecret;
      }

      public String getGrantType() {
            return grantType;
      }

      public void setClientId(String clientId) {
            this.clientId = clientId;
      }

      public void setClientSecret(String clientSecret) {
            this.clientSecret = clientSecret;
      }
}
