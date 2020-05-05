package com.oauth.authserver.authserver.service;

public interface AuthService {
      String getTestKey();

      boolean verifyClientInfo(String clientId, String redirectUrl);

      boolean verifyUserInfo(String username, String password);

      String issueAuthCode(String clientId, String redirectUrl);

      String issueToken(String authCode);
}
