package com.oauth.authserver.authserver.service;

public interface AuthService {

      boolean verifyClientInfo(String clientId, String redirectUrl);

      boolean verifyClientInfo(String clientId, String redirectUrl, String clientSecret, String grantType);

      boolean verifyUserInfo(String username, String password);

      String issueAuthCode(String clientId, String redirectUrl, String username);

      String issueToken(String authCode);

      boolean isExpired(String authCode, String clientId, String redirectUrl);

      boolean matchesCodeClient(String authCode, String clientId, String redirectUrl);

      boolean checkTokenMatches(String accessToken, String clientId, String clientSecret);

      boolean checkExpiredToken(String accessToken);
}
